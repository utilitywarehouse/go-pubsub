package kafka

import (
	"github.com/nats-io/nats"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*messageSink)(nil)

type messageSink struct {
	topic string

	conn *nats.Conn
}

func NewNatsMessageSink(topic string, natsURL string) (pubsub.MessageSink, error) {

	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}

	return &messageSink{
		topic: topic,
		conn:  conn,
	}, nil
}

func (mq *messageSink) PutMessage(m pubsub.Message) error {
	println("publishing message to nats")
	return mq.conn.Publish(mq.topic, m.Data)
}

func (mq *messageSink) Close() error {
	mq.conn.Close()
	return nil
}
