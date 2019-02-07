package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/utilitywarehouse/go-pubsub"
	"golang.org/x/sync/errgroup"
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
	offsetsRetention         time.Duration
	Version                  *sarama.KafkaVersion
}

type MessageSourceConfig struct {
	ConsumerGroup            string
	Topic                    string
	Brokers                  []string
	Offset                   int64
	MetadataRefreshFrequency time.Duration
	OffsetsRetention         time.Duration
	Version                  *sarama.KafkaVersion
}
// NewMessageSource provides a new kafka message source
func NewMessageSource(config MessageSourceConfig) pubsub.ConcurrentMessageSource {
	offset := OffsetLatest
	if config.Offset != 0 {
		offset = config.Offset
	}

	mrf := defaultMetadataRefreshFrequency
	if config.MetadataRefreshFrequency > 0 {
		mrf = config.MetadataRefreshFrequency
	}

	return &messageSource{
		consumergroup:            config.ConsumerGroup,
		topic:                    config.Topic,
		brokers:                  config.Brokers,
		offset:                   offset,
		offsetsRetention:         config.OffsetsRetention,
		metadataRefreshFrequency: mrf,
		Version:                  config.Version,
	}
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = mq.offset
	config.Metadata.RefreshFrequency = mq.metadataRefreshFrequency
	config.Consumer.Offsets.Retention = mq.offsetsRetention

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
			return nil
		}
	}
}

// ConsumeMessagesConcurrently consumes messages concurrently through the use of separate go-routines
// in the context of Kafka this is done by providing a new routine by partition made available to
// the application by kafka at runtime
func (mq *messageSource) ConsumeMessagesConcurrently(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = mq.offset
	config.Metadata.RefreshFrequency = mq.metadataRefreshFrequency
	config.Consumer.Offsets.Retention = mq.offsetsRetention
	config.Group.Mode = cluster.ConsumerModePartitions

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

	pGroup, pContext := errgroup.WithContext(ctx)

	for {
		select {
		case part, ok := <-c.Partitions():
			// partitions will emit a nil pointer (part) when the parent channel is tombed as
			// the client is closed. ok will be false when the partitions channel is closed
			// in both cases we want to wait for the errgroup to handle any errors correctly and
			// gracefully close the subroutines. If either the part is nil or ok is false then
			// we simply ignore it to give the errgroup and subroutines time to finish
			if ok && part != nil {
				pGroup.Go(newConcurrentMessageHandler(pContext, c, part, handler, onError))
			}
		case err := <-c.Errors():
			pGroup.Wait()
			return err
		case <- ctx.Done():
			return pGroup.Wait()
		case <- pContext.Done():
			return pGroup.Wait()
		}
	}
}

func newConcurrentMessageHandler(ctx context.Context,
		consumer *cluster.Consumer,
		part cluster.PartitionConsumer,
		handler pubsub.ConsumerMessageHandler,
		onError pubsub.ConsumerErrorHandler) func() error {

	return func() error {
		for {
			select {
			case msg := <- part.Messages():
				if msg == nil {
					continue
				}
				message := pubsub.ConsumerMessage{Data: msg.Value}
				err := handler(message)
				if err != nil {
					err = onError(message, err)
					if err != nil {
						return err
					}
				}
				consumer.MarkOffset(msg, "")
			case err := <- part.Errors():
				return err
			case <- ctx.Done():
				return nil
			}
		}
	}
}

// Status reports the status of the message source
func (mq *messageSource) Status() (*pubsub.Status, error) {
	return status(mq.brokers, mq.topic)
}
