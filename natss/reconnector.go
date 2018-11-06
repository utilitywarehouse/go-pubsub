package natss

import (
	"github.com/utilitywarehouse/go-pubsub"
	"sync"
	"time"
)

var _ pubsub.MessageSink = (*ReconnectSink)(nil)

type ReconnectSink struct {
	sync.RWMutex
	sink          pubsub.MessageSink
	needReconnect chan struct{}

	connectSink func() (pubsub.MessageSink, error)
}

func NewReconectorSink(connectSink func() (pubsub.MessageSink, error)) (pubsub.MessageSink, error) {
	pubsubsink, err := connectSink()

	if err != nil {
		return nil, err
	}

	reconnectSink := &ReconnectSink{
		sink:          pubsubsink,
		needReconnect: make(chan struct{}),
		connectSink:   connectSink,
	}

	go reconnectSink.reconnect()

	return reconnectSink, nil
}

func (mq *ReconnectSink) PutMessage(m pubsub.ProducerMessage) error {
	mq.RLock()
	defer mq.RUnlock()
	err := mq.sink.PutMessage(m)
	if err != nil {
		mq.needReconnect <- struct{}{}
	}
	return err
}

func (mq *ReconnectSink) Close() error {
	mq.RLock()
	defer mq.RUnlock()
	return mq.sink.Close()
}

func (mq *ReconnectSink) Status() (*pubsub.Status, error) {
	mq.RLock()
	defer mq.RUnlock()
	return mq.sink.Status()
}

func (mq *ReconnectSink) setSink(s pubsub.MessageSink) {
	mq.Lock()
	defer mq.Unlock()
	if mq.sink != nil {
		_ = mq.sink.Close()
	}
	mq.sink = s
}

func (mq *ReconnectSink) reconnect() {

	for {
		// wait until we need to reconnect
		<-mq.needReconnect
		t := time.NewTicker(time.Second * 2)
	reconnectLoop:
		for {
			<-t.C

			newSink, error := mq.connectSink()

			if error != nil {
				// Do nothing. TODO: Add optional logging here?
			} else {
				mq.setSink(newSink)
				t.Stop()
				break reconnectLoop
			}
		}
	}
}
