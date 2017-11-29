package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	client                   *cluster.Client
	consumer                 *cluster.Consumer
}

type MessageSourceConfig struct {
	ConsumerGroup            string
	Topic                    string
	Brokers                  []string
	Offset                   int64
	MetadataRefreshFrequency time.Duration
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
	}
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {

	config := cluster.Config{}

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = mq.offset
	config.Metadata.RefreshFrequency = mq.metadataRefreshFrequency

	client, err := cluster.NewClient(mq.brokers, &config)
	if err != nil {
		return err
	}
	mq.client = client
	defer client.Close()

	c, err := cluster.NewConsumerFromClient(client, mq.consumergroup, []string{mq.topic})
	if err != nil {
		return err
	}
	mq.consumer = c
	defer c.Close()

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
	status := &pubsub.Status{
		Working: true,
	}
	subs := mq.consumer.Subscriptions()

	partitions, ok := subs[mq.topic]
	if !ok {
		return nil, errors.New("cannot get consumer subscriptions")
	}

	for _, partition := range partitions {
		num, err := mq.client.InSyncReplicas(mq.topic, partition)
		if err != nil {
			return nil, fmt.Errorf("cannot get InSyncReplicas topic: %s, partiton: %d, err: %v", mq.topic, partition, err)
		}
		if len(num) <= 0 {
			status.Problems = append(status.Problems, fmt.Sprintf("error partition InSyncReplicas is 0, topic: %s, partition: %d, isr: %v", mq.topic, partition, num))
		}
	}
	return status, nil
}
