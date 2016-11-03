package natss

import (
	"github.com/nats-io/go-nats-streaming"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*NatsMessageSink)(nil)

// NatsMessageSink is a MessageSink based on a nats queue
type NatsMessageSink struct {
	topic string

	conn stan.Conn
}

func NewMessageSink(topic string, consumerID string, natsURL string) (pubsub.MessageSink, error) {

	conn, err := stan.Connect("cluster-id", consumerID, stan.NatsURL(natsURL))
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
