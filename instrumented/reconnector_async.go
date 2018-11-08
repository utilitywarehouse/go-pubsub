package instrumented

import (
	"sync"
	"time"

	pubsub "github.com/utilitywarehouse/go-pubsub"
)

func newAsyncReconnectorSink(
	newSink func() (pubsub.MessageSink, error),
	options *ReconnectionOptions,
) (pubsub.MessageSink, error) {

	pubSubSink, err := newSink()

	if err != nil {
		return nil, err
	}

	rs := &asyncReconnectSink{
		newSink:       newSink,
		options:       options,
		sink:          pubSubSink,
		needReconnect: make(chan struct{}),
	}

	go rs.reconnectIfNeeded()

	return rs, nil
}

// syncReconnectSink represents a Sink decorator
// that enhance others sink with retry logic
// mechanism in case the connection drops
type asyncReconnectSink struct {
	sync.RWMutex
	newSink func() (pubsub.MessageSink, error)

	options       *ReconnectionOptions
	sink          pubsub.MessageSink
	needReconnect chan struct{}
}

// PutMessage decorates the pubsub.MessageSink interface making
// aware the sink of disconnection errors
func (mq *asyncReconnectSink) PutMessage(m pubsub.ProducerMessage) error {
	mq.RLock()
	defer mq.RUnlock()

	err := mq.sink.PutMessage(m)

	if err != nil {
		status, errStatus := mq.Status()
		if errStatus == nil && status.Working == true {
			return mq.PutMessage(m)
		}
	}

	return err
}

// Close decorates the pubsub.MessageSink interface making
// aware the sink of disconnection errors
func (mq *asyncReconnectSink) Close() error {
	mq.RLock()
	defer mq.RUnlock()
	return mq.sink.Close()
}

// Status decorates the pubsub.MessageSink interface making
// aware the sink of disconnection errors
func (mq *asyncReconnectSink) Status() (*pubsub.Status, error) {
	mq.RLock()
	defer mq.RUnlock()

	status, err := mq.sink.Status()

	if err != nil {
		return status, err
	}

	if status.Working == false {
		mq.needReconnect <- struct{}{}
	}

	return status, err
}

func (mq *asyncReconnectSink) setSink(s pubsub.MessageSink) (sink pubsub.MessageSink, err error) {
	mq.Lock()
	defer mq.Unlock()
	if mq.sink != nil {
		err = mq.sink.Close()
	}
	mq.sink = s

	return s, err
}

func (mq *asyncReconnectSink) reconnectIfNeeded() {

	reconnecting := false
	reconnected := make(chan struct{})

	for {
		select {
		case <-mq.needReconnect:
			if reconnecting != true {
				reconnecting = true
				go mq.reconnect(reconnected)
			}
		case <-reconnected:
			reconnecting = false
		}
	}
}

func (mq *asyncReconnectSink) reconnect(reconnected chan struct{}) {
	t := time.NewTicker(time.Second * 2)
	for {
		<-t.C

		newSink, err := mq.newSink()

		if err != nil {
			// Fire OnReconnectFailed if we have a func passed
			if mq.options.OnReconnectFailed != nil {
				mq.options.OnReconnectFailed(err)
			}
		} else {
			sink, err := mq.setSink(newSink)

			// Fire OnReconnectSuccess if we have a func passed
			if mq.options != nil && mq.options.OnReconnectSuccess != nil {
				mq.options.OnReconnectSuccess(sink, err)
			}

			if reconnected != nil {
				reconnected <- struct{}{}
			}

			t.Stop()

			break
		}
	}
}
