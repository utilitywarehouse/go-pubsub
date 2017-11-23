package sqs

import (
	"context"
	"time"

	"github.com/pkg/errors"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// Message is the structure of an SQS Message
type Message struct {
	Type             string     `json:"Type"`
	MessageID        string     `json:"MessageId"`
	TopicArn         string     `json:"TopicArn"`
	Subject          string     `json:"Subject"`
	Message          string     `json:"Message"`
	Timestamp        *time.Time `json:"Timestamp"`
	SignatureVersion string     `json:"SignatureVersion"`
	Signature        string     `json:"Signature"`
	SigningCertURL   string     `json:"SigningCertURL"`
	UnsubscribeURL   string     `json:"UnsubscribeURL"`
}

// Consumer polls messages from SQS
type Consumer struct {
	q          queue
	receiveErr error // populated when receiving messages from SQS fails
	// WaitSeconds is the wait time in seconds to wait between API polling requests.
	// If used, will make fewer requests per day which results in smaller AWS bill.
	// Defaults to 0 which effectively disables this.
	WaitSeconds time.Duration
}

// ConsumerError holds ID of failed message to process
type ConsumerError struct {
	MsgID string
	Value error
}

// NewConsumer returns a new instance of SQS Consumer.
func NewConsumer(q queue) *Consumer {
	return &Consumer{q: q}
}

// ConsumeMessages polls messages from SQS
func (c *Consumer) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler,
	onError pubsub.ConsumerErrorHandler) error {

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(c.WaitSeconds * time.Second)
			msgs, err := c.q.ReceiveMessage()
			c.receiveErr = nil

			if err != nil {
				c.receiveErr = err
				return err
			}

			for _, msg := range msgs {
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

				// once the message has been successfully consumed, we can delete it
				if err := c.q.DeleteMessage(msg.ReceiptHandle); err != nil {
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
func (c *Consumer) Status() (*pubsub.Status, error) {
	s := pubsub.Status{Working: true}

	if c.receiveErr != nil {
		s.Working = false
		s.Problems = append(s.Problems, c.receiveErr.Error())
	}

	return &s, nil
}

// Marshal marshals sqs.Message.Message to []byte.
func (m *Message) Marshal() ([]byte, error) {
	return []byte(m.Message), nil
}
