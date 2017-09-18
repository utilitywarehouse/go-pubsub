package natss

import (
	"errors"

	"github.com/nats-io/go-nats-streaming"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*messageSink)(nil)

type MessageSinkConfig struct {
	NatsURL   string
	ClusterID string
	Topic     string
	ClientID  string
}

type messageSink struct {
	topic string

	conn stan.Conn
}

func NewMessageSink(config MessageSinkConfig) (pubsub.MessageSink, error) {

	conn, err := stan.Connect(config.ClusterID, config.ClientID, stan.NatsURL(config.NatsURL))
	if err != nil {
		return nil, err
	}

	return &messageSink{
		topic: config.Topic,
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
	return mq.conn.Close()
}

func (mq *messageSink) Status() (*pubsub.Status, error) {
	return nil, errors.New("status is not implemented")
}
