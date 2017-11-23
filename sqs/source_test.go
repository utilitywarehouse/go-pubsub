package sqs_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	pubsub "github.com/utilitywarehouse/go-pubsub"
	pubSQS "github.com/utilitywarehouse/go-pubsub/sqs"
)

func Test_ConsumeMessagesMissingClient_Fail(t *testing.T) {
	_, err := pubSQS.NewMessageSource(pubSQS.MessageSourceConfig{})
	if err == nil {
		t.Fatal("expected error about missing SQS client, got: <nil>")
	}

	if !strings.Contains(err.Error(), "SQS client must not be nil") {
		t.Errorf("expected error about missing SQS client: got: %v", err)
	}
}

// context canceled
func Test_ConsumeMessagesCtxCancelled_Fail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	handler := func(pubsub.ConsumerMessage) error { return nil }
	errHandler := func(pubsub.ConsumerMessage, error) error { return nil }

	timer := time.NewTimer(10 * time.Millisecond)
	go func() {
		<-timer.C
		cancel()
	}()

	c, err := pubSQS.NewMessageSource(pubSQS.MessageSourceConfig{
		Client: &mockQueue{},
	})
	if err != nil {
		t.Fatalf("failed to get message source: %v", err)
	}

	err = c.ConsumeMessages(ctx, handler, errHandler)
	if err == nil {
		t.Error("expected context cancelation error, got <nil>")
	}

	if !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("expected context cancelation error, got: %v", err)
	}
}

// failed to receive messages, working = false, and 1st problem matches fake error
func Test_ConsumeMessages_Fail(t *testing.T) {
	ctx := context.Background()
	handler := func(pubsub.ConsumerMessage) error { return nil }
	errHandler := func(pubsub.ConsumerMessage, error) error { return nil }

	fakeErr := errors.New("failed to connect to SQS")
	c, err := pubSQS.NewMessageSource(pubSQS.MessageSourceConfig{
		Client: &mockQueue{receiveErr: fakeErr},
	})
	if err != nil {
		t.Fatalf("failed to get message source: %v", err)
	}

	err = c.ConsumeMessages(ctx, handler, errHandler)
	if errors.Cause(err) != fakeErr {
		t.Errorf("expected fake error, got: %v", err)
	}

	st, err := c.Status()
	if err != nil {
		t.Fatalf("unexpected error while trying to get status: %v", err)
	}

	if st.Working {
		t.Errorf("expected status to be not working")
	}

	if len(st.Problems) != 1 {
		t.Fatalf("expected 1 status problem, got: %d: %v", len(st.Problems), st.Problems)
	}

	if st.Problems[0] != fakeErr.Error() {
		t.Fatalf("expected 1st problem to be a fake error when polling, got: %v", st.Problems[0])
	}
}

// handler and onError fails
func Test_ConsumeMessages_Handler_Fail(t *testing.T) {
	ctx := context.Background()
	handler := func(pubsub.ConsumerMessage) error { return errors.New("could not handle message") }
	fakeErr := errors.New("error handler returned fake error")
	errHandler := func(pubsub.ConsumerMessage, error) error { return fakeErr }

	var msgs []*sqs.Message
	msgID, pld := "12345", "some payload for sqs"
	msg := sqs.Message{
		MessageId: &msgID,
		Body:      &pld,
	}
	msgs = append(msgs, &msg)

	c, err := pubSQS.NewMessageSource(pubSQS.MessageSourceConfig{
		Client: &mockQueue{messages: msgs},
	})
	if err != nil {
		t.Fatalf("failed to get message source: %v", err)
	}

	err = c.ConsumeMessages(ctx, handler, errHandler)
	if errors.Cause(err) != fakeErr {
		t.Errorf("expected fake error, got: %v", err)
	}

	st, err := c.Status()
	if err != nil {
		t.Fatalf("unexpected error while trying to get status: %v", err)
	}

	if !st.Working {
		t.Errorf("expected status to be working")
	}

	if len(st.Problems) != 0 {
		t.Fatalf("expected 0 status problem, got: %d: %v", len(st.Problems), st.Problems)
	}
}

// handler succeeds, message fails to be deleted
func Test_ConsumeMessages_DeleteMessage_Fail(t *testing.T) {
	ctx := context.Background()
	handler := func(pubsub.ConsumerMessage) error { return nil }
	errHandler := func(pubsub.ConsumerMessage, error) error { return nil }

	var msgs []*sqs.Message
	msgID, pld := "12345", "some payload for sqs"
	msg := sqs.Message{
		MessageId: &msgID,
		Body:      &pld,
	}
	msgs = append(msgs, &msg)

	fakeErr := errors.New("couldn't delete message")
	c, err := pubSQS.NewMessageSource(pubSQS.MessageSourceConfig{
		Client: &mockQueue{
			messages:  msgs,
			deleteErr: fakeErr,
		},
	})
	if err != nil {
		t.Fatalf("failed to get message source: %v", err)
	}

	err = c.ConsumeMessages(ctx, handler, errHandler)
	if err == nil {
		t.Fatal("expected message deletion error, got <nil>")
	}

	if errors.Cause(err) != fakeErr {
		t.Errorf("expected sqs response error, got: %v", err)
	}

	if !strings.Contains(err.Error(), "failed to delete SQS message") {
		t.Errorf("expected message deletion error, got: %v", err)
	}

	st, err := c.Status()
	if err != nil {
		t.Fatalf("unexpected error while trying to get status: %v", err)
	}

	// when we fail to delete a message we do not degrade status
	if !st.Working {
		t.Errorf("expected status to be working")
	}

	if len(st.Problems) != 0 {
		t.Fatalf("expected 0 status problem, got: %d: %v", len(st.Problems), st.Problems)
	}
}

// handler succeeds, message is deleted successfully
func Test_ConsumeMessages_DeleteMessage_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	handler := func(pubsub.ConsumerMessage) error { return nil }
	errHandler := func(pubsub.ConsumerMessage, error) error { return nil }

	var msgs []*sqs.Message
	msgID, pld := "12345", "some payload for sqs"
	msg := sqs.Message{
		MessageId: &msgID,
		Body:      &pld,
	}
	msgs = append(msgs, &msg)

	timer := time.NewTimer(time.Millisecond)
	go func() {
		<-timer.C
		cancel()
	}()

	c, err := pubSQS.NewMessageSource(pubSQS.MessageSourceConfig{
		Client: &mockQueue{messages: msgs},
	})
	if err != nil {
		t.Fatalf("failed to get message source: %v", err)
	}

	err = c.ConsumeMessages(ctx, handler, errHandler)
	if err == nil {
		t.Fatal("expected cancelation error, got <nil>")
	}

	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected cancelation error, got: %v", err)
	}

	st, err := c.Status()
	if err != nil {
		t.Fatalf("unexpected error while trying to get status: %v", err)
	}

	if !st.Working {
		t.Errorf("expected status to be working")
	}

	if len(st.Problems) != 0 {
		t.Fatalf("expected 0 status problem, got: %d: %v", len(st.Problems), st.Problems)
	}
}

func Test_ConsumerError_Success(t *testing.T) {
	msgID, value := "12345", errors.New("couldn't handle message")

	err := pubSQS.ConsumerError{
		MsgID: msgID,
		Value: value,
	}

	if err.Error() != value.Error() {
		t.Errorf("unexpected consumer error value, expected: %v, got: %v", value, err.Error())
	}
}
