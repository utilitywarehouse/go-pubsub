package kafka

import (
	"context"
	"time"

	"github.com/bsm/sarama-cluster"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

const (
	OffsetOldest int64 = -2
	OffsetLatest int64 = -1
)

type messageSource struct {
	consumergroup string
	topic         string
	brokers       []string
	offset        int64
}

type MessageSourceConfig struct {
	ConsumerGroup string
	Topic         string
	//Deprecated: use Brokers instead
	Zookeepers []string
	Brokers    []string
	Offset     int64
}

func NewMessageSource(config MessageSourceConfig) pubsub.MessageSource {
	brokers := config.Brokers
	if brokers == nil {
		brokers = config.Zookeepers
	}

	offset := OffsetLatest
	if config.Offset != 0 {
		offset = config.Offset
	}

	return &messageSource{
		consumergroup: config.ConsumerGroup,
		topic:         config.Topic,
		brokers:       brokers,
		offset:        offset,
	}
}

var processingTimeout = 60 * time.Second

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = mq.offset
	c, err := cluster.NewConsumer(mq.brokers, mq.consumergroup, []string{mq.topic}, config)
	if err != nil {
		return err
	}

	defer func() {
		_ = c.Close()
	}()

	for {
		select {
		case msg := <-c.Messages():
			message := pubsub.ConsumerMessage{msg.Value}
			err := handler(message)
			if err != nil {
				err := onError(message, err)
				if err != nil {
					return err
				}
			}

			c.MarkOffset(msg, "")
		case err := <-c.Errors():
			return err
		case <-ctx.Done():
			return c.Close()
		}
	}
}
