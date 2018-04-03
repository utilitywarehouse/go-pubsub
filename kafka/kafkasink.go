package kafka

import (
	"errors"
	"sync"

	"time"

	"github.com/Shopify/sarama"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*messageSink)(nil)

type messageSink struct {
	topic string

	keyFunc func(pubsub.ProducerMessage) []byte

	lk       sync.Mutex
	producer sarama.SyncProducer
	closed   bool

	brokers []string
}

type MessageSinkConfig struct {
	Topic           string
	Brokers         []string
	KeyFunc         func(pubsub.ProducerMessage) []byte
	MaxMessageBytes *int
}

func NewMessageSink(config MessageSinkConfig) (pubsub.MessageSink, error) {

	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	conf.Producer.Retry.Max = 3
	conf.Producer.Timeout = time.Duration(60) * time.Second

	if config.MaxMessageBytes != nil {
		conf.Producer.MaxMessageBytes = *config.MaxMessageBytes
	}

	if config.KeyFunc != nil {
		conf.Producer.Partitioner = sarama.NewHashPartitioner
	} else {
		conf.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}

	producer, err := sarama.NewSyncProducer(config.Brokers, conf)
	if err != nil {
		return nil, err
	}

	return &messageSink{
		topic:    config.Topic,
		producer: producer,
		keyFunc:  config.KeyFunc,
		brokers:  config.Brokers,
	}, nil
}

func (mq *messageSink) PutMessage(m pubsub.ProducerMessage) error {
	message := &sarama.ProducerMessage{
		Topic: mq.topic,
	}

	data, err := m.Marshal()
	if err != nil {
		return err
	}
	message.Value = sarama.ByteEncoder(data)
	if mq.keyFunc != nil {
		message.Key = sarama.ByteEncoder(mq.keyFunc(m))
	}

	_, _, err = mq.producer.SendMessage(message)
	return err
}

func (mq *messageSink) Close() error {
	mq.lk.Lock()
	defer mq.lk.Unlock()

	if mq.closed {
		return errors.New("Already closed")
	}

	mq.closed = true
	return mq.producer.Close()
}

// Status reports the status of the message sink
func (mq *messageSink) Status() (*pubsub.Status, error) {
	return status(mq.brokers)
}
