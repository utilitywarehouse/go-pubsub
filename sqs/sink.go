package sqs

import (
	"sync"

	"github.com/pkg/errors"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// Sink holds SQS dependency and other flags that are necessary to identify its status.
type Sink struct {
	q       queue
	sinkErr error // holds error when SQS poll failed
	lk      sync.Mutex
	closed  bool
}

// NewSink is the constructor returning a *Sink.
func NewSink(q queue) *Sink {
	return &Sink{q: q}
}

// PutMessage publishes a sqs.Message to SQS.
func (s *Sink) PutMessage(msg pubsub.ProducerMessage) error {
	if s.closed {
		return errors.New("sqs connection closed")
	}

	// this is needed to mark status as healthy if a temporary issue is resolved
	s.sinkErr = nil

	marshalledMsg, err := msg.Marshal()
	if err != nil {
		return errors.Wrapf(err, "failed to marshal sqs message: %+v", msg)
	}

	payload := string(marshalledMsg)
	if err := s.q.SendMessage(&payload); err != nil {
		s.sinkErr = err
		return errors.Wrap(err, "failed to sink message in sqs")
	}

	return nil
}

// Status reports whether sink is healthy or not. Instead of doing API requests
// we wait for SendMessage to fail to degrade the status.
func (s *Sink) Status() (*pubsub.Status, error) {
	status := pubsub.Status{Working: true}
	if s.sinkErr != nil {
		status.Working = false
		status.Problems = append(status.Problems, s.sinkErr.Error())
	}

	return &status, nil
}

// Close is added to satisfy MessageSink interface.
func (s *Sink) Close() error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if s.closed {
		return errors.New("Already closed")
	}

	// there's no connection to close, so we set closed flag to true and
	// prevent sending messages to SQS.
	s.closed = true

	return nil
}
