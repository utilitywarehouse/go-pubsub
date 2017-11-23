package sqs

import (
	"sync"

	awsSQS "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// MessageSinkConfig allows you to set sink options.
type MessageSinkConfig struct {
	Client   Queue
	QueueURL *string
}

type messageSink struct {
	q         Queue
	sinkErr   error // holds error when SQS poll failed
	lk        sync.Mutex
	closed    bool
	sendInput *awsSQS.SendMessageInput
}

// NewMessageSink is the sink constructor. If SQS client is nil, an error is returned.
func NewMessageSink(config MessageSinkConfig) (pubsub.MessageSink, error) {
	if config.Client == nil {
		return nil, errMissingClient
	}

	return &messageSink{
		q:         config.Client,
		sendInput: &awsSQS.SendMessageInput{QueueUrl: config.QueueURL},
	}, nil
}

// PutMessage sinks a pubsub.ProducerMessage in SQS.
func (s *messageSink) PutMessage(msg pubsub.ProducerMessage) error {
	if s.closed {
		return errors.New("SQS connection closed")
	}

	// this is needed to mark status as healthy if a temporary issue is resolved
	s.sinkErr = nil

	marshalledMsg, err := msg.Marshal()
	if err != nil {
		return errors.Wrapf(err, "failed to marshal SQS message: %+v", msg)
	}

	payload := string(marshalledMsg)

	sendInput := s.sendInput
	sendInput.MessageBody = &payload

	if _, err := s.q.SendMessage(sendInput); err != nil {
		s.sinkErr = err
		return errors.Wrap(err, "failed to sink message in SQS")
	}

	return nil
}

// Status reports whether sink is healthy or not. Instead of doing API requests
// we wait for SendMessage to fail to degrade the status.
func (s *messageSink) Status() (*pubsub.Status, error) {
	status := pubsub.Status{Working: true}
	if s.sinkErr != nil {
		status.Working = false
		status.Problems = append(status.Problems, s.sinkErr.Error())
	}

	return &status, nil
}

// Close is added to satisfy MessageSink interface.
func (s *messageSink) Close() error {
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
