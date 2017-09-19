package proximo

import (
	"context"
	"errors"
	"sync"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/uw-labs/proximo/proximoc-go"
)

var _ pubsub.MessageSink = (*messageSink)(nil)

type messageSink struct {
	topic string

	keyFunc func(pubsub.ProducerMessage) []byte

	lk       sync.Mutex
	producer *proximoc.ProducerConn
	closed   bool

	broker string
}

type MessageSinkConfig struct {
	Topic  string
	Broker string
}

func NewMessageSink(config MessageSinkConfig) (pubsub.MessageSink, error) {

	ctx := context.Background()
	producer, err := proximoc.DialProducer(ctx, config.Broker, config.Topic)

	if err != nil {
		return nil, err
	}

	return &messageSink{
		topic:    config.Topic,
		producer: producer,
		broker:   config.Broker,
	}, nil
}

func (mq *messageSink) PutMessage(m pubsub.ProducerMessage) error {
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	return mq.producer.Produce(data)
}

func (mq *messageSink) Close() error {
	mq.lk.Lock()
	defer mq.lk.Unlock()

	if mq.closed {
		return errors.New("Already closed")
	}

	mq.closed = true
	return mq.producer.Close()
}

// Status reports the status of the message sink
func (mq *messageSink) Status() (*pubsub.Status, error) {
	return nil, errors.New("status is not implemented")
}
