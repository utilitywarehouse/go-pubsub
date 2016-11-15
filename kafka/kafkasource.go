package kafka

import (
	"errors"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/wvanbergen/kafka/consumergroup"
)

var _ pubsub.MessageSource = (*messageSource)(nil)

type messageSource struct {
	consumergroup string
	topic         string
	zookeepers    []string

	close  chan struct{}
	closed chan struct{}
}

func NewMessageSource(consumergroup, topic string, zookeepers []string) pubsub.MessageSource {
	return &messageSource{
		consumergroup: consumergroup,
		topic:         topic,
		zookeepers:    zookeepers,

		close:  make(chan struct{}),
		closed: make(chan struct{}),
	}
}

func (mq *messageSource) ConsumeMessages(handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {

	conf := consumergroup.NewConfig()

	cg, err := consumergroup.JoinConsumerGroup(mq.consumergroup, []string{mq.topic}, mq.zookeepers, conf)
	if err != nil {
		return err
	}

	defer close(mq.closed)

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
		case <-mq.close:
			return cg.Close()
		}
	}
}

func (mq *messageSource) Close() error {
	select {
	case <-mq.closed:
		return errors.New("Already closed")
	case mq.close <- struct{}{}:
		<-mq.closed
		return nil
	}
}
