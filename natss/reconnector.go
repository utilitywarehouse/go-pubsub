package natss

import (
	"github.com/utilitywarehouse/go-pubsub"
	"sync"
	"time"
)

var _ pubsub.MessageSink = (*ReconnectSink)(nil)

type ReconnectSink struct {
	sync.RWMutex
	sink        pubsub.MessageSink
	connecting  bool
	connectSink func() (pubsub.MessageSink, error)
}

func NewReconectorSink(connectSink func() (pubsub.MessageSink, error)) (pubsub.MessageSink, error) {
	pubsubsink, err := connectSink()

	if err != nil {
		return nil, err
	}

	reconnectSink := &ReconnectSink{
		sink:        pubsubsink,
		connecting:  false,
		connectSink: connectSink,
	}

	return reconnectSink, nil
}

func (mq *ReconnectSink) PutMessage(m pubsub.ProducerMessage) error {
	err := mq.sink.PutMessage(m)

	if err == ErrConnLost {
		go mq.reconnect()
	}

	return err
}

func (mq *ReconnectSink) Close() error {
	return mq.sink.Close()
}

func (mq *ReconnectSink) Status() (*pubsub.Status, error) {
	return mq.sink.Status()
}

func (mq *ReconnectSink) reconnect() (newSink pubsub.MessageSink, error error) {
	attempts := 0
	maxAttempts := 0

	t := time.NewTicker(time.Second * 2)

	if mq.connecting == true {
		return nil, nil
	}

	for {
		<-t.C

		if attempts >= maxAttempts {
			return nil, error
		}

		mq.RLock()
		mq.connecting = true

		newSink, error = mq.connectSink()

		if error != nil {
			attempts++
		} else {
			mq.sink = newSink
		}

		mq.connecting = false
		mq.RUnlock()
	}
}
