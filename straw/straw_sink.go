package straw

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/uw-labs/straw"
)

var _ pubsub.MessageSink = (*messageSink)(nil)

type messageSink struct {
	streamstore straw.StreamStore
	path        string

	reqs chan *messageReq

	maxUnflushedTime time.Duration

	exitErr  error
	closeReq chan struct{}
	closed   chan struct{}
}

type MessageSinkConfig struct {
	Path             string
	MaxUnflushedTime time.Duration
}

const (
	DefaultMaxUnflushedTime = time.Second * 10
)

func NewMessageSink(streamstore straw.StreamStore, config MessageSinkConfig) (pubsub.MessageSink, error) {

	if config.MaxUnflushedTime == 0 {
		config.MaxUnflushedTime = DefaultMaxUnflushedTime
	}

	_, err := streamstore.Stat(config.Path)
	if os.IsNotExist(err) {
		if err := straw.MkdirAll(streamstore, config.Path, 0755); err != nil {
			return nil, err
		}
		_, err = streamstore.Stat(config.Path)
	}
	if err != nil {
		return nil, err
	}

	ms := &messageSink{
		streamstore: streamstore,
		path:        config.Path,
		reqs:        make(chan *messageReq),

		maxUnflushedTime: config.MaxUnflushedTime,

		closeReq: make(chan struct{}),
		closed:   make(chan struct{}),
	}

	nextSeq, err := nextSequence(ms.streamstore, config.Path)
	if err != nil {
		return nil, err
	}

	go ms.loop(nextSeq)

	return ms, nil
}

func (mq *messageSink) loop(nextSeq int) {
	writtenCount := 0
	var t *time.Timer
	var timerC <-chan time.Time

	var wc io.WriteCloser

	for {
		if t == nil {
			timerC = nil
		} else {
			timerC = t.C
		}
		select {
		case r := <-mq.reqs:
			data, err := r.m.Marshal()
			if err != nil {
				mq.exitErr = err
				close(mq.closed)
				r.errs <- err
			}
			var lenBytes [4]byte
			binary.LittleEndian.PutUint32(lenBytes[:], uint32(len(data)))
			if wc == nil {
				nextFile := seqToPath(mq.path, nextSeq)
				if err := straw.MkdirAll(mq.streamstore, filepath.Dir(nextFile), 0755); err != nil {
					mq.exitErr = err
					close(mq.closed)
					r.errs <- err
					return
				}
				wc, err = mq.streamstore.CreateWriteCloser(nextFile)
				if err != nil {
					mq.exitErr = err
					close(mq.closed)
					r.errs <- err
					return
				}
			}
			if _, err := wc.Write(lenBytes[:]); err != nil {
				mq.exitErr = err
				close(mq.closed)
				r.errs <- err
			}
			if _, err := wc.Write(data); err != nil {
				mq.exitErr = err
				close(mq.closed)
				r.errs <- err
			}
			if t == nil {
				t = time.NewTimer(mq.maxUnflushedTime)
			}
			r.errs <- nil
			writtenCount++
		case <-timerC:
			t = nil
			if _, err := wc.Write([]byte{0, 0, 0, 0}); err != nil {
				mq.exitErr = err
				close(mq.closed)
			}
			if err := wc.Close(); err != nil {
				mq.exitErr = err
				close(mq.closed)
			}
			nextSeq++
			wc = nil
			writtenCount = 0
		case <-mq.closeReq:
			if wc != nil {
				if _, err := wc.Write([]byte{0, 0, 0, 0}); err != nil {
					mq.exitErr = err
					close(mq.closed)
				}
				mq.exitErr = wc.Close()
			}
			close(mq.closed)
			return
		}
	}
}

type messageReq struct {
	m    pubsub.ProducerMessage
	errs chan error
}

func (mq *messageSink) PutMessage(m pubsub.ProducerMessage) error {
	req := &messageReq{m, make(chan error, 1)}
	select {
	case mq.reqs <- req:
		return <-req.errs
	case <-mq.closed:
		return mq.exitErr
	}
}

func (mq *messageSink) Close() error {
	select {
	case mq.closeReq <- struct{}{}:
		<-mq.closed
		return mq.exitErr
	case <-mq.closed:
		return errors.New("already closed")
	}
}

// Status reports the status of the message sink
func (mq *messageSink) Status() (*pubsub.Status, error) {
	panic("implement me!")
}
