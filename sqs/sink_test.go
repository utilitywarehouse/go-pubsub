package sqs_test

import (
	"strings"
	"testing"

	awsSQS "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	pubSQS "github.com/utilitywarehouse/go-pubsub/sqs"
)

func Test_PutMessageMissingClient_Fail(t *testing.T) {
	_, err := pubSQS.NewMessageSink(pubSQS.MessageSinkConfig{})
	if err == nil {
		t.Fatal("expected error about missing SQS client, got: <nil>")
	}

	if !strings.Contains(err.Error(), "SQS client must not be nil") {
		t.Errorf("expected error about missing SQS client: got: %v", err)
	}
}

func Test_PutMessage_Success(t *testing.T) {
	s, err := pubSQS.NewMessageSink(pubSQS.MessageSinkConfig{
		Client: &mockQueue{},
	})
	if err != nil {
		t.Fatalf("failed toget SQS message sink: %v", err)
	}

	msg := message{payload: "test string"}
	if err := s.PutMessage(&msg); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func Test_PutMessageSinkClosed_Fail(t *testing.T) {
	s, err := pubSQS.NewMessageSink(pubSQS.MessageSinkConfig{
		Client: &mockQueue{},
	})
	if err != nil {
		t.Fatalf("failed toget SQS message sink: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("unexpected error while closing sink: %v", err)
	}

	err = s.PutMessage(&message{})
	if !strings.Contains(err.Error(), "SQS connection closed") {
		t.Errorf("expected error about closed connection, got: %v", err)
	}
}

func Test_PutMessageInvalid_Fail(t *testing.T) {
	fakeErr := errors.New("fake error")
	s, err := pubSQS.NewMessageSink(pubSQS.MessageSinkConfig{
		Client: &mockQueue{},
	})
	if err != nil {
		t.Fatalf("failed toget SQS message sink: %v", err)
	}

	err = s.PutMessage(&mockMessage{err: fakeErr})
	if err == nil {
		t.Fatalf("expected marshalling error, got: <nil>")
	}

	if errors.Cause(err) != fakeErr {
		t.Errorf("expected fake error, got: %v", err)
	}

	if !strings.Contains(err.Error(), "failed to marshal SQS message") {
		t.Errorf("expected failed marshalling error, got: %v", err)
	}

	st, err := s.Status()
	if err != nil {
		t.Errorf("unexpected status error: %v", err)
	}

	if !st.Working {
		t.Errorf("status working should be true because SQS didn't fail")
	}

	if len(st.Problems) != 0 {
		t.Errorf("expected 0 status problems, got: %d: %v", len(st.Problems), st.Problems)
	}
}

func Test_PutMessage_Fail(t *testing.T) {
	fakeErr := errors.New("fake error")
	s, err := pubSQS.NewMessageSink(pubSQS.MessageSinkConfig{
		Client: &mockQueue{sendErr: fakeErr},
	})

	if err != nil {
		t.Fatalf("failed toget SQS message sink: %v", err)
	}

	err = s.PutMessage(&message{})
	if errors.Cause(err) != fakeErr {
		t.Errorf("expected fake error, got: %v", err)
	}

	st, err := s.Status()
	if err != nil {
		t.Errorf("unexpected status error: %v", err)
	}

	if st.Working {
		t.Errorf("status working should be false because SQS failed")
	}

	if len(st.Problems) != 1 {
		t.Errorf("expected 1 status problems, got: %d: %v", len(st.Problems), st.Problems)
	}

	if st.Problems[0] != fakeErr.Error() {
		t.Errorf("status problem should be fake error, got: %v", st.Problems[0])
	}
}

func Test_PutMessageAlreadyClosed_Fail(t *testing.T) {
	s, err := pubSQS.NewMessageSink(pubSQS.MessageSinkConfig{
		Client: &mockQueue{},
	})

	if err != nil {
		t.Fatalf("failed toget SQS message sink: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("unexpected error while closing sink: %v", err)
	}

	err = s.Close()
	if err == nil {
		t.Fatalf("expected error about already closed connection")
	}

	if !strings.Contains(err.Error(), "already closed") {
		t.Errorf("expected closed error, got: %v", err)
	}
}

type mockMessage struct {
	err error
}

func (mm *mockMessage) Marshal() ([]byte, error) {
	return nil, mm.err
}

type mockQueue struct {
	messages   []*awsSQS.Message
	receiveErr error
	deleteErr  error
	sendErr    error
}

func (m *mockQueue) SendMessage(input *awsSQS.SendMessageInput) (*awsSQS.SendMessageOutput, error) {
	return nil, m.sendErr
}

func (m *mockQueue) ReceiveMessage(input *awsSQS.ReceiveMessageInput) (*awsSQS.ReceiveMessageOutput, error) {
	return &awsSQS.ReceiveMessageOutput{
		Messages: m.messages,
	}, m.receiveErr
}

func (m *mockQueue) DeleteMessage(input *awsSQS.DeleteMessageInput) (*awsSQS.DeleteMessageOutput, error) {
	return nil, m.deleteErr
}

type message struct {
	payload string
	err     error
}

func (msg *message) Marshal() ([]byte, error) {
	return []byte(msg.payload), msg.err
}
