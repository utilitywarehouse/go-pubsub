package straw

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/uw-labs/straw"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

type messageSource struct {
	streamstore straw.StreamStore
	path        string
	pollPeriod  time.Duration
}

type MessageSourceConfig struct {
	Path       string
	PollPeriod time.Duration
}

func NewMessageSource(streamstore straw.StreamStore, config MessageSourceConfig) pubsub.MessageSource {
	ms := &messageSource{
		streamstore: streamstore,
		path:        config.Path,
		pollPeriod:  config.PollPeriod,
	}
	if ms.pollPeriod == 0 {
		ms.pollPeriod = 5 * time.Second
	}
	return ms
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	/*
		fi, err := mq.streamstore.Stat(mq.path)
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("%v is not a directory", fi.Name())
		}
	*/

	for seq := 0; ; seq++ {
		fullname := seqToPath(mq.path, seq)

		var rc io.ReadCloser
		var err error

	waitLoop:
		for {
			rc, err = mq.streamstore.OpenReadCloser(fullname)
			if err == nil {
				break waitLoop
			}
			if !os.IsNotExist(err) {
				return err
			}
			t := time.NewTimer(mq.pollPeriod)
			select {
			case <-ctx.Done():
				t.Stop()
				if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
					return nil
				}
				return ctx.Err()
			case <-t.C:
			}
		}
	readLoop:
		for {
			lenBytes := []byte{0, 0, 0, 0}
			if _, err := rc.Read(lenBytes[:]); err != nil {
				return err
			}
			len := int(binary.LittleEndian.Uint32(lenBytes[:]))
			if len == 0 {
				// next read should be EOF
				buf := []byte{0}
				if _, err := rc.Read(buf); err != io.EOF {
					return fmt.Errorf("Was able to read past end marker. This is broken, bailing out.")
				}
				break readLoop
			}
			buf := make([]byte, len)
			if _, err := io.ReadFull(rc, buf); err != nil {
				return err
			}
			message := pubsub.ConsumerMessage{Data: buf}
			if err := handler(message); err != nil {
				err = onError(message, err)
				if err != nil {
					return err
				}
			}
		}
	}
}

// Status reports the status of the message source
func (mq *messageSource) Status() (*pubsub.Status, error) {
	panic("implement me!")
}
