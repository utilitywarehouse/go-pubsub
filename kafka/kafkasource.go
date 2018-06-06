package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

const (
	OffsetOldest                    int64 = -2
	OffsetLatest                    int64 = -1
	defaultMetadataRefreshFrequency       = 10 * time.Minute
)

type messageSource struct {
	consumergroup            string
	topic                    string
	brokers                  []string
	offset                   int64
	metadataRefreshFrequency time.Duration
	Version                  *sarama.KafkaVersion
}

type MessageSourceConfig struct {
	ConsumerGroup            string
	Topic                    string
	Brokers                  []string
	Offset                   int64
	MetadataRefreshFrequency time.Duration
	Version                  *sarama.KafkaVersion
}

func NewMessageSource(config MessageSourceConfig) pubsub.MessageSource {
	offset := OffsetLatest
	if config.Offset != 0 {
		offset = config.Offset
	}
	mrf := defaultMetadataRefreshFrequency
	if config.MetadataRefreshFrequency > 0 {
		mrf = config.MetadataRefreshFrequency
	}

	return &messageSource{
		consumergroup: config.ConsumerGroup,
		topic:         config.Topic,
		brokers:       config.Brokers,
		offset:        offset,
		metadataRefreshFrequency: mrf,
		Version:                  config.Version,
	}
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = mq.offset
	config.Metadata.RefreshFrequency = mq.metadataRefreshFrequency

	if mq.Version != nil {
		config.Version = *mq.Version
	}

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
			message := pubsub.ConsumerMessage{Data: msg.Value}
			err := handler(message)
			if err != nil {
				err = onError(message, err)
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

// Status reports the status of the message source
func (mq *messageSource) Status() (*pubsub.Status, error) {
	return status(mq.brokers, mq.topic)
}
