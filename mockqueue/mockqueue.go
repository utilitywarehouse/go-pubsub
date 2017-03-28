package mockqueue

import (
	"context"

	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*MockQueue)(nil)
var _ pubsub.MessageSource = (*MockQueue)(nil)

// MockQueue is intended for testing purposes.
// It provides a simple in-memory sink and source
type MockQueue struct {
	q      chan []byte
	close  chan struct{}
	closed chan struct{}
}

func NewMockQueue() *MockQueue {
	return &MockQueue{make(chan []byte, 9999), make(chan struct{}), make(chan struct{})}
}

func (mq *MockQueue) PutMessage(m pubsub.ProducerMessage) error {
	data, err := m.MarshalPubSub()
	if err != nil {
		return err
	}
	mq.q <- data
	return nil
}

func (mq *MockQueue) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-mq.q:
			cm := pubsub.ConsumerMessage{m}
			err := handler(cm)
			if err != nil {
				if err := onError(cm, err); err != nil {
					return err
				}
			}
		}
	}
}

func (mq *MockQueue) Close() error {
	// no-op in mock.
	return nil
}
