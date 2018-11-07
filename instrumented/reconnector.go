package instrumented

import (
	"github.com/utilitywarehouse/go-pubsub"
	"sync"
	"time"
)

var _ pubsub.MessageSink = (*ReconnectSink)(nil)

// ReconnectSink represents a Sink decorator
// that enhance others sink with retry logic
// mechanism in case the connection drops
type ReconnectSink struct {
	sync.RWMutex
	newSink     func() (pubsub.MessageSink, error)
	errConnLost error

	options       *ReconnectionOptions
	sink          pubsub.MessageSink
	needReconnect chan struct{}
}

// ReconnectionOptions represents the options
// to configure and hook up to reconnection events
type ReconnectionOptions struct {
	ReconnectSynchronously bool
	OnReconnectFailed  func(err error)
	OnReconnectSuccess func(sink pubsub.MessageSink, err error)
}


// NewReconnectorSink creates a new reconnector sink
func NewReconnectorSink(
	errConnLost error,
	newSink func() (pubsub.MessageSink, error),
	options *ReconnectionOptions,
) (pubsub.MessageSink, error) {
	pubSubSink, err := newSink()

	if err != nil {
		return nil, err
	}

	reconnectSink := &ReconnectSink{
		newSink:       newSink,
		errConnLost:   errConnLost,
		options:       options,
		sink:          pubSubSink,
		needReconnect: make(chan struct{}),
	}

	go reconnectSink.reconnect()

	return reconnectSink, nil
}

// PutMessage decorates the pubsub.MessageSink interface making
// aware the sink of disconnection errors
func (mq *ReconnectSink) PutMessage(m pubsub.ProducerMessage) error {
	if mq.options.ReconnectSynchronously == false {
		mq.RLock()
		defer mq.RUnlock()
	}

	err := mq.sink.PutMessage(m)

	if err != nil && err == mq.errConnLost {
		if mq.options.ReconnectSynchronously == true {
			mq.Lock()
			mq.retryStrategy(nil)
			mq.Unlock()
			return mq.sink.PutMessage(m)
		}
		mq.needReconnect <- struct{}{}
	}

	return err
}

// Close decorates the pubsub.MessageSink interface making
// aware the sink of disconnection errors
func (mq *ReconnectSink) Close() error {
	if mq.options.ReconnectSynchronously == false {
		mq.RLock()
		defer mq.RUnlock()
	}
	return mq.sink.Close()
}

// Status decorates the pubsub.MessageSink interface making
// aware the sink of disconnection errors
func (mq *ReconnectSink) Status() (*pubsub.Status, error) {
	if mq.options.ReconnectSynchronously == false {
		mq.RLock()
		defer mq.RUnlock()
	}

	status, err := mq.sink.Status()

	if err != nil {
		return status, err
	}

	if status.Working == false {
		if mq.options.ReconnectSynchronously == true {
			mq.Lock()
			mq.retryStrategy(nil)
			mq.Unlock()
			return mq.Status()
		}

		mq.needReconnect <- struct{}{}
	}

	return status, err
}

func (mq *ReconnectSink) setSink(s pubsub.MessageSink) (sink pubsub.MessageSink, err error) {
	if mq.options.ReconnectSynchronously == false {
		mq.Lock()
		defer mq.Unlock()
	}
	if mq.sink != nil {
		err = mq.sink.Close()
	}
	mq.sink = s

	return s, err
}

func (mq *ReconnectSink) reconnect() {
	// If the reconnector is synchronous
	// we can just clean up this go routine
	if mq.options.ReconnectSynchronously == true {
		close(mq.needReconnect)
		return
	}

	reconnecting := false
	reconnected := make(chan struct{})

	for {
		select {
		case <-mq.needReconnect:
			if reconnecting != true {
				reconnecting = true
				go mq.retryStrategy(reconnected)
			}
		case <-reconnected:
			reconnecting = false
		}
	}
}

func (mq *ReconnectSink) retryStrategy(reconnected chan struct{}) {
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
