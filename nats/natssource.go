package kafka

import (
	"context"

	"github.com/nats-io/nats"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

type messageSource struct {
	topic   string
	natsURL string
}

func NewNatsMessageSource(topic string, natsURL string) (pubsub.MessageSource, error) {

	return &messageSource{
		topic:   topic,
		natsURL: natsURL,
	}, nil
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {

	conn, err := nats.Connect(mq.natsURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch := make(chan *nats.Msg, 64)
	sub, err := conn.ChanSubscribe(mq.topic, ch)
	if err != nil {
		return err
	}

	for {
		select {
		case m := <-ch:
			msg := pubsub.ConsumerMessage{m.Data}
			err := handler(msg)
			if err != nil {
				if err := onError(msg, err); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return sub.Unsubscribe()
		}
	}

	/*
		mq.conn.Subscribe(mq.topic, func(m *nats.Msg) {
			message := pubsub.Message{m.Data}
			err := handler(message)
			if err != nil {
				if err := onError(message, err); err != nil {
					return err
				}
			}
		})
	*/
}
