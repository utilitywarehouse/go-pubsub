package kafka

import (
	"errors"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/utilitywarehouse/go-pubsub"
)

var (
	broker = flag.String("broker", "", "kafka broker address")
	zk     = flag.String("zk", "", "zookeeper address")
)

func TestMain(m *testing.M) {
	flag.Parse()
	processingTimeout = 1 * time.Second
	result := m.Run()
	os.Exit(result)
}

func TestSimpleProduceConsume(t *testing.T) {
	if *broker == "" || *zk == "" {
		t.Skip("kafka and zookeeper not configured")
	}

	topicName := uuid.NewRandom().String()
	message := uuid.NewRandom().String()

	produceMessage(t, topicName, message)

	msg := make(chan string, 1)

	cons := NewMessageSource(MessageSourceConfig{"test-group", topicName, []string{*zk}})
	defer cons.Close()

	go func() {
		handler := func(m pubsub.ConsumerMessage) error {
			msg <- string(m.Data)
			close(msg)
			return nil
		}

		onError := func(m pubsub.ConsumerMessage, e error) error {
			t.Error("unexpected error")
			return nil
		}
		if err := cons.ConsumeMessages(handler, onError); err != nil {
			t.Error(err)
		}
	}()

	select {
	case m := <-msg:
		if m != message {
			t.Error("message not as expected")
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for message")
	}
}

func TestConsumeError(t *testing.T) {
	if *broker == "" || *zk == "" {
		t.Skip("kafka and zookeeper not configured")
	}

	topicName := uuid.NewRandom().String()
	message := uuid.NewRandom().String()

	produceMessage(t, topicName, message)

	{
		cons := NewMessageSource(MessageSourceConfig{"test-group", topicName, []string{*zk}})

		doneMsg := make(chan struct{})

		handler := func(m pubsub.ConsumerMessage) error {
			return errors.New("test error")
		}

		onError := func(m pubsub.ConsumerMessage, e error) error {
			close(doneMsg)
			return errors.New("onError error")
		}

		consumeErr := make(chan error, 1)
		go func() {
			consumeErr <- cons.ConsumeMessages(handler, onError)
		}()

		<-doneMsg
		err := cons.Close()
		if err == nil {
			t.Error("Expected error due to being already closed")
		}

		if consumeErr == nil {
			t.Error("expected error")
		}
	}

	{
		// message should be still available to consume
		cons2 := NewMessageSource(MessageSourceConfig{"test-group", topicName, []string{*zk}})
		defer cons2.Close()

		msg := make(chan string, 1)

		go func() {
			handler := func(m pubsub.ConsumerMessage) error {
				msg <- string(m.Data)
				close(msg)
				return nil
			}

			onError := func(m pubsub.ConsumerMessage, e error) error {
				t.Error("unexpected error")
				return nil
			}
			if err := cons2.ConsumeMessages(handler, onError); err != nil {
				t.Error(err)
			}
		}()

		select {
		case m := <-msg:
			if m != message {
				t.Error("message not as expected")
			}
		case <-time.After(2 * time.Second):
			t.Error("timeout waiting for message")
		}
	}
}

func produceMessage(t *testing.T, topicName, message string) {
	sink, err := NewMessageSink(MessageSinkConfig{topicName, []string{*broker}, nil})
	if err != nil {
		t.Fatal(err)
	}

	sink.PutMessage(pubsub.SimpleProducerMessage([]byte(message)))

	if err := sink.Close(); err != nil {
		t.Error(err)
	}
}
