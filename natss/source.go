package natss

import (
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSource = (*NatsMessageSource)(nil)

// MessageSource is a MessageSource based on a nats queue
type NatsMessageSource struct {
	topic string
	conn  stan.Conn
}

func NewMessageSource(clusterID, topic, consumerID, natsURL string) (pubsub.MessageSource, error) {

	conn, err := stan.Connect(clusterID, consumerID, stan.NatsURL(natsURL))
	if err != nil {
		return nil, err
	}

	return &NatsMessageSource{
		topic: topic,
		conn:  conn,
	}, nil
}

func (mq *NatsMessageSource) ConsumeMessages(handler pubsub.MessageHandler, onError pubsub.ErrorHandler) error {

	f := func(msg *stan.Msg) {
		m := pubsub.Message{msg.Data}
		err := handler(m)
		if err != nil {
			if err := onError(m, err); err != nil {
				panic("write the error handling")
			}
		}
	}

	startOpt := stan.StartAt(pb.StartPosition_NewOnly)

	groupName := "groupName"

	_, err := mq.conn.QueueSubscribe("demo-topic", groupName, f, startOpt, stan.DurableName(groupName))
	if err != nil {
		return err
	}

	return nil
}

func (mq *NatsMessageSource) Close() error {
	return mq.conn.Close()
}
