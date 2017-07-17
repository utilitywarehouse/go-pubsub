package natss

import (
	"errors"

	"github.com/nats-io/go-nats-streaming"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*messageSink)(nil)

type messageSink struct {
	topic string

	conn stan.Conn
}

func NewMessageSink(clusterID, topic, consumerID, natsURL string) (pubsub.MessageSink, error) {

	conn, err := stan.Connect(clusterID, consumerID, stan.NatsURL(natsURL))
	if err != nil {
		return nil, err
	}

	return &messageSink{
		topic: topic,
		conn:  conn,
	}, nil
}

func (mq *messageSink) PutMessage(m pubsub.ProducerMessage) error {
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	return mq.conn.Publish(mq.topic, data)
}

func (mq *messageSink) Close() error {
	mq.conn.Close()
	return nil
}

func (mq *messageSink) Status() (*pubsub.Status, error) {
	return nil, errors.New("status is not implemented")
}
