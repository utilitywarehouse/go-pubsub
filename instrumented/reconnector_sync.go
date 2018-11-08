package instrumented

import (
	"sync"
	"time"

	pubsub "github.com/utilitywarehouse/go-pubsub"
)

func newSyncReconnectorSink(
	newSink func() (pubsub.MessageSink, error),
	options *ReconnectionOptions,
) (pubsub.MessageSink, error) {

	pubSubSink, err := newSink()

	if err != nil {
		return nil, err
	}

	rs := &syncReconnectSink{
		newSink: newSink,
		options: options,
		sink:    pubSubSink,
	}

	return rs, nil
}

// syncReconnectSink represents a Sink decorator
// that enhance others sink with retry logic
// mechanism in case the connection drops
type syncReconnectSink struct {
	newSink func() (pubsub.MessageSink, error)
	options *ReconnectionOptions

	sync.RWMutex
	sink pubsub.MessageSink
}

// PutMessage decorates the pubsub.MessageSink interface making
// aware the sink of disconnection errors
func (mq *syncReconnectSink) PutMessage(m pubsub.ProducerMessage) error {

	for {
		err := mq.getSink().PutMessage(m)
		if err == nil {
			return nil
		}
		reconnected := mq.reconnectIfNeeded()
		if !reconnected {
			return err
		}
	}

}

func (mq *syncReconnectSink) reconnectIfNeeded() (wasNeeded bool) {
	status, err := mq.Status()
	if err == nil && status.Working == true {
		// already connected
		return false
	}
	mq.reconnect()
	return true
}

// Close decorates the pubsub.MessageSink interface making
// aware the sink of disconnection errors
func (mq *syncReconnectSink) Close() error {
	return mq.getSink().Close()
}

// Status decorates the pubsub.MessageSink interface making
// aware the sink of disconnection errors
func (mq *syncReconnectSink) Status() (*pubsub.Status, error) {
	return mq.getSink().Status()
}

func (mq *syncReconnectSink) getSink() pubsub.MessageSink {
	mq.RLock()
	defer mq.RUnlock()
	return mq.sink
}

func (mq *syncReconnectSink) setSink(s pubsub.MessageSink) (sink pubsub.MessageSink, err error) {
	mq.Lock()
	defer mq.Unlock()

	if mq.sink != nil {
		err = mq.sink.Close()
	}
	mq.sink = s

	return s, err
}

func (mq *syncReconnectSink) reconnect() {
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

			t.Stop()

			break
		}
	}
}
