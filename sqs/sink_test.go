package sqs_test

import (
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/utilitywarehouse/go-pubsub/sqs"
)

func Test_PutMessage_Success(t *testing.T) {
	s := sqs.NewSink(&mockSink{})
	msg := sqs.Message{Message: "test string"}
	if err := s.PutMessage(&msg); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func Test_PutMessageSinkClosed_Fail(t *testing.T) {
	s := sqs.NewSink(&mockSink{})
	if err := s.Close(); err != nil {
		t.Fatalf("unexpected error while closing sink: %v", err)
	}

	err := s.PutMessage(&sqs.Message{})
	if !strings.Contains(err.Error(), "sqs connection closed") {
		t.Errorf("expected error about closed connection, got: %v", err)
	}
}

func Test_PutMessageInvalid_Fail(t *testing.T) {
	fakeErr := errors.New("fake error")
	s := sqs.NewSink(&mockSink{})

	err := s.PutMessage(&mockMessage{err: fakeErr})
	if err == nil {
		t.Fatalf("expected marshalling error, got: <nil>")
	}

	if errors.Cause(err) != fakeErr {
		t.Errorf("expected fake error, got: %v", err)
	}

	if !strings.Contains(err.Error(), "failed to marshal sqs message") {
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
	s := sqs.NewSink(&mockSink{err: fakeErr})

	err := s.PutMessage(&sqs.Message{})
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
	s := sqs.NewSink(&mockSink{})
	if err := s.Close(); err != nil {
		t.Fatalf("unexpected error while closing sink: %v", err)
	}

	err := s.Close()
	if err == nil {
		t.Fatalf("expected error about already closed connection")
	}

	if !strings.Contains(err.Error(), "Already closed") {
		t.Errorf("expected Already closed error, got: %v", err)
	}
}

type mockMessage struct {
	err error
}

func (mm *mockMessage) Marshal() ([]byte, error) {
	return nil, mm.err
}

type mockSink struct {
	err error
}

func (m *mockSink) SendMessage(pld *string) error {
	return m.err
}
