package kafka

import (
	"errors"
	"sync"

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
}

func NewMessageSink(topic string, brokers []string, keyFunc func(pubsub.ProducerMessage) []byte) (pubsub.MessageSink, error) {

	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Partitioner = sarama.NewHashPartitioner

	producer, err := sarama.NewSyncProducer(brokers, conf)
	if err != nil {
		return nil, err
	}

	return &messageSink{
		topic:    topic,
		producer: producer,
		keyFunc:  keyFunc,
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
	message.Key = sarama.ByteEncoder(mq.keyFunc(m))

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
