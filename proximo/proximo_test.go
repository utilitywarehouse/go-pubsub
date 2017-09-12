package proximo

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/utilitywarehouse/go-pubsub"
)

func TestMain(m *testing.M) {
	flag.Parse()

	cmd := exec.Command("proximo-server", "--port=6869", "mem")

	ep, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}

	if err := cmd.Start(); err != nil {
		panic(err)
	}
	time.Sleep(200 * time.Millisecond)

	result := m.Run()

	if result != 0 {
		io.Copy(os.Stderr, ep)
	}

	cmd.Process.Kill()

	os.Exit(result)
}

func TestSimpleProduceConsume(t *testing.T) {

	topicName := uuid.NewRandom().String()
	message := uuid.NewRandom().String()

	produceMessage(t, topicName, message)

	msg := make(chan string, 1)

	cons := NewMessageSource(MessageSourceConfig{ConsumerGroup: "test-group", Topic: topicName, Broker: "localhost:6869"})

	ctx, canc := context.WithCancel(context.Background())
	defer canc()

	handler := func(m pubsub.ConsumerMessage) error {
		msg <- string(m.Data)
		canc()
		close(msg)
		return nil
	}

	onError := func(m pubsub.ConsumerMessage, e error) error {
		t.Error("unexpected error")
		return nil
	}

	if err := cons.ConsumeMessages(ctx, handler, onError); err != nil {
		t.Error(err)
	}

	select {
	case <-time.After(2 * time.Second):
		t.Error("didn't get message")
	case m := <-msg:
		if m != message {
			t.Error("message not as expected")
		}
	}
}

func TestConsumeError(t *testing.T) {

	topicName := uuid.NewRandom().String()
	message := uuid.NewRandom().String()

	produceMessage(t, topicName, message)

	{
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		cons := NewMessageSource(MessageSourceConfig{ConsumerGroup: "test-group", Topic: topicName, Broker: "localhost:6869"})

		handler := func(m pubsub.ConsumerMessage) error {
			return errors.New("test error")
		}

		onError := func(m pubsub.ConsumerMessage, e error) error {
			return errors.New("onError error")
		}

		consumeErr := cons.ConsumeMessages(ctx, handler, onError)
		if consumeErr == nil {
			t.Error("expected error")
		}
	}

	{
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// message should be still available to consume
		cons2 := NewMessageSource(MessageSourceConfig{ConsumerGroup: "test-group", Topic: topicName, Broker: "localhost:6869"})

		msg := make(chan string, 1)

		handler := func(m pubsub.ConsumerMessage) error {
			msg <- string(m.Data)
			close(msg)
			cancel()
			return nil
		}

		onError := func(m pubsub.ConsumerMessage, e error) error {
			t.Error("unexpected error")
			return nil
		}

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
	sink, err := NewMessageSink(MessageSinkConfig{topicName, "localhost:6869"})
	if err != nil {
		t.Fatal(err)
	}

	sink.PutMessage(pubsub.SimpleProducerMessage([]byte(message)))

	if err := sink.Close(); err != nil {
		t.Error(err)
	}
}
