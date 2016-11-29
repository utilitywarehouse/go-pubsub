package natss

import (
	"context"

	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

type messageSource struct {
	natsURL    string
	clusterID  string
	consumerID string
	topic      string
}

func NewMessageSource(natsURL, clusterID, consumerID, topic string) (pubsub.MessageSource, error) {
	return &messageSource{
		natsURL:    natsURL,
		clusterID:  clusterID,
		consumerID: consumerID,
		topic:      topic,
	}, nil
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {

	conn, err := stan.Connect(mq.clusterID, mq.consumerID, stan.NatsURL(mq.natsURL))
	if err != nil {
		return err
	}
	defer conn.Close()

	f := func(msg *stan.Msg) {
		m := pubsub.ConsumerMessage{msg.Data}
		err := handler(m)
		if err != nil {
			if err := onError(m, err); err != nil {
				panic("write the error handling")
			}
		}
	}

	startOpt := stan.StartAt(pb.StartPosition_First)

	_, err = conn.QueueSubscribe(mq.topic, mq.consumerID, f, startOpt, stan.DurableName(mq.consumerID))
	if err != nil {
		return err
	}

	//	s.Unsubscribe()
	<-ctx.Done()
	conn.Close()

	return nil
}
