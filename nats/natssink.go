package kafka

import (
	"github.com/nats-io/nats"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*NatsMessageSink)(nil)

// NatsMessageSink is a MessageSink based on a nats queue
type NatsMessageSink struct {
	topic string

	conn *nats.Conn
}

func NewNatsMessageSink(topic string, natsURL string) (pubsub.MessageSink, error) {

	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}

	return &NatsMessageSink{
		topic: topic,
		conn:  conn,
	}, nil
}

func (mq *NatsMessageSink) PutMessage(m pubsub.Message) error {
	println("publishing message to nats")
	return mq.conn.Publish(mq.topic, m.Data)
}

func (mq *NatsMessageSink) Close() error {
	mq.conn.Close()
	return nil
}
