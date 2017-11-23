package sqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awsSQS "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// MessageSourceConfig allows you to configure various source options.
type MessageSourceConfig struct {
	// WaitSeconds is the wait time in seconds to wait between API polling requests.
	// If used, will make fewer requests per day which results in smaller AWS bill.
	// Defaults to 0 which effectively disables this.
	WaitSeconds time.Duration
	Client      Queue
	QueueURL    string
}

var errMissingClient = errors.New("SQS client must not be nil")

type messageSource struct {
	q            Queue
	receiveErr   error // populated when receiving messages from SQS fails
	receiveInput *awsSQS.ReceiveMessageInput
	deleteInput  *awsSQS.DeleteMessageInput
	waitSeconds  time.Duration
}

// ConsumerError satisfies error interface and is returned when a message fails to be
// handled. If you do a type switch on the error, you can extract the MsgID.
type ConsumerError struct {
	MsgID string
	Value error
}

// NewMessageSource is the source constructor. If a nil client is passed an error is returned.
func NewMessageSource(config MessageSourceConfig) (pubsub.MessageSource, error) {
	if config.Client == nil {
		return nil, errMissingClient
	}

	return &messageSource{
		q:           config.Client,
		waitSeconds: config.WaitSeconds,
		receiveInput: &awsSQS.ReceiveMessageInput{
			QueueUrl:            aws.String(config.QueueURL),
			AttributeNames:      aws.StringSlice([]string{"All"}),
			MaxNumberOfMessages: aws.Int64(1),
		},
		deleteInput: &awsSQS.DeleteMessageInput{QueueUrl: aws.String(config.QueueURL)},
	}, nil
}

// ConsumeMessages polls messages from SQS
func (s *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler,
	onError pubsub.ConsumerErrorHandler) error {

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(s.waitSeconds * time.Second)
			msgs, err := s.q.ReceiveMessage(s.receiveInput)
			s.receiveErr = nil

			if err != nil {
				s.receiveErr = err
				return err
			}

			for _, msg := range msgs.Messages {
				consumerMsg := pubsub.ConsumerMessage{Data: []byte(*msg.Body)}

				if err := handler(consumerMsg); err != nil {
					msgErr := ConsumerError{
						MsgID: *msg.MessageId,
						Value: err,
					}

					if innerErr := onError(consumerMsg, &msgErr); innerErr != nil {
						return innerErr
					}
				}

				delInput := s.deleteInput
				delInput.ReceiptHandle = msg.ReceiptHandle

				// once the message has been successfully consumed, delete it
				if _, err := s.q.DeleteMessage(delInput); err != nil {
					return errors.Wrap(err, "failed to delete SQS message")
				}
			}
		}
	}
}

// Error method satisfies Error interface of standard library for ConsumerError struct.
func (e *ConsumerError) Error() string {
	return e.Value.Error()
}

// Status method is used to understand if the service is healthy or not.
// In order to prevent making unnecessary API requests to AWS we rely on
// sqs.ReceiveMessage error to set `receiveErr` field in Consumer.
func (s *messageSource) Status() (*pubsub.Status, error) {
	st := pubsub.Status{Working: true}

	if s.receiveErr != nil {
		st.Working = false
		st.Problems = append(st.Problems, s.receiveErr.Error())
	}

	return &st, nil
}
