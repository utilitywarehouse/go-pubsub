package amqp

import (
	"errors"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*messageSink)(nil)

type messageSink struct {
	topic string

	lk      sync.Mutex
	closed  bool
	channel *amqp.Channel
	q       amqp.Queue
	conn    *amqp.Connection
}

type MessageSinkConfig struct {
	Topic   string
	Address string
}

func NewMessageSink(config MessageSinkConfig) (pubsub.MessageSink, error) {

	conn, err := amqp.Dial(config.Address)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		config.Topic, // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		return nil, err
	}

	return &messageSink{
		topic: config.Topic,

		conn:    conn,
		q:       q,
		channel: ch,
	}, nil
}

func (mq *messageSink) PutMessage(m pubsub.ProducerMessage) error {

	data, err := m.MarshalPubSub()
	if err != nil {
		return err
	}

	p := amqp.Publishing{
		Body: data,
	}

	return mq.channel.Publish(
		"",        // exchange
		mq.q.Name, // routing key
		true,      // mandatory
		false,     // immediate
		p,
	)
}

func (mq *messageSink) Close() error {
	mq.lk.Lock()
	defer mq.lk.Unlock()

	if mq.closed {
		return errors.New("Already closed")
	}

	mq.closed = true

	err1 := mq.channel.Close()
	err2 := mq.conn.Close()
	if err1 != nil {
		if err2 != nil {
			return fmt.Errorf("errors closing amqp channel and connection :\n%v\n%v\n", err1, err2)
		}
		return err1
	}
	return err2
}
func (mq *messageSink) Status() (*pubsub.Status, error) {
	return nil, errors.New("status is not implemented")

}
