package kafka

import (
	"github.com/nats-io/nats"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

type messageSource struct {
	topic string

	conn *nats.Conn

	close  chan struct{}
	closed chan struct{}
}

func NewNatsMessageSource(topic string, natsURL string) (pubsub.MessageSource, error) {

	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}

	return &messageSource{
		topic: topic,
		conn:  conn,

		close:  make(chan struct{}),
		closed: make(chan struct{}),
	}, nil
}

func (mq *messageSource) ConsumeMessages(handler pubsub.MessageHandler, onError pubsub.ErrorHandler) error {

	ch := make(chan *nats.Msg, 64)
	sub, err := mq.conn.ChanSubscribe(mq.topic, ch)
	if err != nil {
		return err
	}

	defer close(mq.closed)

	for {
		select {
		case m := <-ch:
			msg := pubsub.Message{m.Data}
			err := handler(msg)
			if err != nil {
				if err := onError(msg, err); err != nil {
					return err
				}
			}
		case <-mq.close:
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

func (mq *messageSource) Close() error {
	mq.conn.Close()
	return nil
}
