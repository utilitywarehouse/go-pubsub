package kafka

import (
	"context"
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
)

func TestMain(m *testing.M) {
	flag.Parse()
	processingTimeout = 1 * time.Second
	result := m.Run()
	os.Exit(result)
}

func TestNewMessageSourceBackwardsCompatible(t *testing.T) {
	cons := NewMessageSource(MessageSourceConfig{ConsumerGroup: "test-group", Topic: "topic", Zookeepers: []string{"localhost:9092"}}).(*messageSource)
	if cons.consumergroup != "test-group" {
		t.Error("unexpected consumer group")
	}
	if cons.topic != "topic" {
		t.Error("unexpected topic")
	}
	if cons.brokers[0] != "localhost:9092" {
		t.Error("unexpected brokers")
	}
	if cons.offset != OffsetLatest {
		t.Error("unexpected offset")
	}
}

func TestNewMessageSource(t *testing.T) {
	cons := NewMessageSource(MessageSourceConfig{ConsumerGroup: "test-group", Topic: "topic", Brokers: []string{"localhost:9092"}, Offset: OffsetOldest}).(*messageSource)
	if cons.consumergroup != "test-group" {
		t.Error("unexpected consumer group")
	}
	if cons.topic != "topic" {
		t.Error("unexpected topic")
	}

	if cons.brokers[0] != "localhost:9092" {
		t.Error("unexpected brokers")
	}
	if cons.offset != OffsetOldest {
		t.Error("unexpected offset")
	}

}

func TestNewMessageSourceOffsetsDefaultToLatest(t *testing.T) {
	cons := NewMessageSource(MessageSourceConfig{ConsumerGroup: "test-group", Topic: "topic", Brokers: []string{"localhost:9092"}}).(*messageSource)
	if cons.consumergroup != "test-group" {
		t.Error("unexpected consumer group")
	}
	if cons.topic != "topic" {
		t.Error("unexpected topic")
	}
	if cons.brokers[0] != "localhost:9092" {
		t.Error("unexpected brokers")
	}
	if cons.offset != OffsetLatest {
		t.Error("unexpected offset")
	}

}

func TestSimpleProduceConsume(t *testing.T) {
	if *broker == "" {
		t.Skip("kafka not configured")
	}

	topicName := uuid.NewRandom().String()
	message := uuid.NewRandom().String()

	produceMessage(t, topicName, message)

	msg := make(chan string, 1)

	cons := NewMessageSource(MessageSourceConfig{ConsumerGroup: "test-group", Topic: topicName, Brokers: []string{*broker}, Offset: OffsetOldest})

	handler := func(m pubsub.ConsumerMessage) error {
		msg <- string(m.Data)
		close(msg)
		return nil
	}

	onError := func(m pubsub.ConsumerMessage, e error) error {
		t.Error("unexpected error")
		return nil
	}

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

	if err := cons.ConsumeMessages(ctx, handler, onError); err != nil {
		t.Error(err)
	}

	select {
	case m := <-msg:
		if m != message {
			t.Error("message not as expected")
		}
	default:
		t.Error("didn't get message")
	}
}

func TestConsumeError(t *testing.T) {
	if *broker == "" {
		t.Skip("kafka  not configured")
	}

	topicName := uuid.NewRandom().String()
	message := uuid.NewRandom().String()

	produceMessage(t, topicName, message)

	{
		cons := NewMessageSource(MessageSourceConfig{ConsumerGroup: "test-group", Topic: topicName, Brokers: []string{*broker}, Offset: OffsetOldest})

		doneMsg := make(chan struct{})

		handler := func(m pubsub.ConsumerMessage) error {
			return errors.New("test error")
		}

		onError := func(m pubsub.ConsumerMessage, e error) error {
			close(doneMsg)
			return errors.New("onError error")
		}

		ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

		consumeErr := cons.ConsumeMessages(ctx, handler, onError)
		if consumeErr == nil {
			t.Error("expected error")
		}
	}

	{
		// message should be still available to consume
		cons2 := NewMessageSource(MessageSourceConfig{ConsumerGroup: "test-group", Topic: topicName, Brokers: []string{*broker}, Offset: OffsetOldest})

		msg := make(chan string, 1)

		handler := func(m pubsub.ConsumerMessage) error {
			msg <- string(m.Data)
			close(msg)
			return nil
		}

		onError := func(m pubsub.ConsumerMessage, e error) error {
			t.Error("unexpected error")
			return nil
		}

		ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
		if err := cons2.ConsumeMessages(ctx, handler, onError); err != nil {
			t.Error(err)
		}

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
