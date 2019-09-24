package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
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
	lock := &sync.Mutex{}
	return mq.ConsumeMessagesConcurrently(ctx, func(message pubsub.ConsumerMessage) error {
		lock.Lock()
		defer lock.Unlock()

		return handler(message)
	}, onError)
}

// consumerGroupHandler is a handler wrapper for Sarama ConsumerGroupAPI
// sarama will make multiple concurrent calls to ConsumeClaim
type consumerGroupHandler struct {
	ctx     context.Context
	handler pubsub.ConsumerMessageHandler
	onError pubsub.ConsumerErrorHandler
	errChan <-chan error
}

// Setup creates consumer group session
func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup cleans up the consumer group session
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim calls out to pubsub handler
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				// message channel closed so bail cleanly
				return nil
			}

			message := pubsub.ConsumerMessage{Data: msg.Value}
			err := h.handler(message)
			if err != nil {
				err = h.onError(message, err)
				if err != nil {
					return err
				}
			}
			sess.MarkMessage(msg, "")

		case err, ok := <-h.errChan:
			if !ok {
				// error channel closed so bail cleanly. If we don't do this we will kill the
				// consumer every time there is a consumer rebalance
				return nil
			}
			return err

		case <-h.ctx.Done():
			return nil
		}
	}
}

// ConsumeMessagesConcurrently consumes messages concurrently through the use of separate go-routines
// in the context of Kafka this is done by providing a new routine by partition made available to
// the application by kafka at runtime
func (mq *messageSource) ConsumeMessagesConcurrently(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = mq.offset
	config.Metadata.RefreshFrequency = mq.metadataRefreshFrequency
	config.Consumer.Offsets.Retention = mq.offsetsRetention

	if mq.Version != nil {
		config.Version = *mq.Version
	}

	c, err := sarama.NewConsumerGroup(mq.brokers, mq.consumergroup, config)
	if err != nil {
		return err
	}

	defer func() {
		_ = c.Close()
	}()

	h := &consumerGroupHandler{
		ctx:     ctx,
		handler: handler,
		onError: onError,
		errChan: c.Errors(),
	}

	err = c.Consume(ctx, []string{mq.topic}, h)
	if err != nil {
		return err
	}

	return nil
}

// Status reports the status of the message source
func (mq *messageSource) Status() (*pubsub.Status, error) {
	return status(mq.brokers, mq.topic)
}
