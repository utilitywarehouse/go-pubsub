package proximo

import (
	"context"
	"errors"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/uw-labs/proximo/proximoc-go"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

type messageSource struct {
	consumergroup string
	topic         string
	broker        string
}

type MessageSourceConfig struct {
	ConsumerGroup string
	Topic         string
	Broker        string
}

func NewMessageSource(config MessageSourceConfig) pubsub.MessageSource {

	return &messageSource{
		consumergroup: config.ConsumerGroup,
		topic:         config.Topic,
		broker:        config.Broker,
	}
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	cf := func(msg *proximoc.Message) error {
		m := pubsub.ConsumerMessage{msg.GetData()}
		err := handler(m)
		if err != nil {
			err = onError(m, err)
		}
		return err
	}
	return proximoc.ConsumeContext(ctx, mq.broker, mq.consumergroup, mq.topic, cf)
}

// Status reports the status of the message source
func (mq *messageSource) Status() (*pubsub.Status, error) {
	return nil, errors.New("status is not implemented")
}
