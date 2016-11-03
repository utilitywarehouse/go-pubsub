package mockqueue

import (
	"errors"

	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*MockQueue)(nil)
var _ pubsub.MessageSource = (*MockQueue)(nil)

// MockQueue is intended for testing purposes.
// It provides a simple in-memory sink and source
type MockQueue struct {
	q      chan pubsub.Message
	close  chan struct{}
	closed chan struct{}
}

func NewMockQueue() *MockQueue {
	return &MockQueue{make(chan pubsub.Message, 9999), make(chan struct{}), make(chan struct{})}
}

func (mq *MockQueue) PutMessage(m pubsub.Message) error {
	mq.q <- m
	return nil
}

func (mq *MockQueue) ConsumeMessages(handler pubsub.MessageHandler, onError pubsub.ErrorHandler) error {
	for {
		select {
		case <-mq.close:
			close(mq.closed)
			return nil
		case m := <-mq.q:
			err := handler(m)
			if err != nil {
				if err := onError(m, err); err != nil {
					return err
				}
			}
		}
	}
}

func (mq *MockQueue) Close() error {
	select {
	case <-mq.closed:
		return errors.New("Already closed")
	case mq.close <- struct{}{}:
		<-mq.closed
		return nil
	}
}
