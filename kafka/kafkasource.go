package kafka

import (
	"context"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/wvanbergen/kafka/consumergroup"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

type messageSource struct {
	consumergroup string
	topic         string
	zookeepers    []string
}

type MessageSourceConfig struct {
	ConsumerGroup string
	Topic         string
	Zookeepers    []string
}

func NewMessageSource(config MessageSourceConfig) pubsub.MessageSource {
	return &messageSource{
		consumergroup: config.ConsumerGroup,
		topic:         config.Topic,
		zookeepers:    config.Zookeepers,
	}
}

var processingTimeout = 60 * time.Second

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {

	conf := consumergroup.NewConfig()
	conf.Offsets.ProcessingTimeout = processingTimeout

	cg, err := consumergroup.JoinConsumerGroup(mq.consumergroup, []string{mq.topic}, mq.zookeepers, conf)
	if err != nil {
		return err
	}

	defer func() {
		_ = cg.Close()
	}()

	for {
		select {
		case msg := <-cg.Messages():
			message := pubsub.ConsumerMessage{msg.Value}
			err := handler(message)
			if err != nil {
				err := onError(message, err)
				if err != nil {
					return err
				}
			}

			cg.CommitUpto(msg)
		case err := <-cg.Errors():
			return err
		case <-ctx.Done():
			return cg.Close()
		}
	}
}
