package natss

import (
	"github.com/pkg/errors"

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
	sc    stan.Conn // nats streaming
}

func NewMessageSink(config MessageSinkConfig) (pubsub.MessageSink, error) {

	sc, err := stan.Connect(config.ClusterID, config.ClientID, stan.NatsURL(config.NatsURL))
	if err != nil {
		return nil, errors.Wrapf(err, "connecting nats streaming client to cluster: %s at: %s", config.ClusterID, config.NatsURL)
	}

	return &messageSink{
		topic: config.Topic,
		sc:    sc,
	}, nil
}

func (mq *messageSink) PutMessage(m pubsub.ProducerMessage) error {
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	return mq.sc.Publish(mq.topic, data)
}

func (mq *messageSink) Close() error {
	return mq.sc.Close()
}

func (mq *messageSink) Status() (*pubsub.Status, error) {
	return natsStatus(*mq.sc.NatsConn()), nil
}
