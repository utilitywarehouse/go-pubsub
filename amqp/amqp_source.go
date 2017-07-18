package amqp

import (
	"context"
	"errors"

	"github.com/streadway/amqp"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

type messageSource struct {
	consumergroup string
	topic         string
	address       string
}

type MessageSourceConfig struct {
	ConsumerGroup string
	Topic         string
	Address       string
}

func NewMessageSource(config MessageSourceConfig) pubsub.MessageSource {
	return &messageSource{
		consumergroup: config.ConsumerGroup,
		topic:         config.Topic,
		address:       config.Address,
	}
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {

	conn, err := amqp.Dial(mq.address)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		mq.topic, // name
		true,     // durable
		false,    // delete when usused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		q.Name,           // queue
		mq.consumergroup, // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		return err
	}

	for {
		select {
		case msg := <-msgs:
			message := pubsub.ConsumerMessage{msg.Body}
			err := handler(message)
			if err != nil {
				err := onError(message, err)
				if err != nil {
					return err
				}
			}

			msg.Ack(false)
		case <-ctx.Done():
			return ch.Close()
		}
	}
}

func (mq *messageSource) Status() (*pubsub.Status, error) {
	return nil, errors.New("status is not implemented")
}
