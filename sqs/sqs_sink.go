package sqs

import (
	"fmt"
	"net/url"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// MessageSinkConfig configures the sqs transport as a pubsub messasge sink
type MessageSinkConfig struct {
	maxRetries int
	queueURL   string
	awsKey     string
	awsSecret  string
	awsRegion  string
}

// Attributes encapsulates an attribute map to be attached to the message as sqs attributes
type Attributes map[string]string

// Attributable is an interface users can implement to attach attributes to sqs messages
type Attributable interface {
	GetAttributes() Attributes
}

// DefaultMessageSinkConfig sets up a complete messageSinkConfig object, validating input
func DefaultMessageSinkConfig(awsRegion, queueURL string, maxRetries uint8) (*MessageSinkConfig, error) {
	if len(awsRegion) < 1 {
		return nil, fmt.Errorf("awsRegion needs to be set")
	}
	_, err := url.Parse(queueURL)
	if err != nil {
		return nil, fmt.Errorf("queueURL needs to be a valid url: %+v", err)
	}
	return &messageSinkConfig{
		awsRegion:  awsRegion,
		maxRetries: int(maxRetries),
		queueURL:   queueURL,
	}, nil
}

type messageSink struct {
	sqs      *sqs.SQS
	queueURL *string
}

func (m *messageSink) Close() error {
	panic("not implemented")
}

func (m *messageSink) PutMessage(msg pubsub.ProducerMessage) error {
	bodyBytes, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal message: %+v", err)
	}
	bodyStr := string(bodyBytes)
	attributes := make(map[string]*sqs.MessageAttributeValue)
	if attributable, ok := msg.(Attributable); ok {
		for k, v := range attributable.GetAttributes() {
			attributes[k] = &sqs.MessageAttributeValue{
				StringValue: &v,
			}
		}
	}
	sqsMessage := &sqs.SendMessageInput{
		QueueUrl:    m.queueURL,
		MessageBody: &bodyStr,
	}
	r, err := m.sqs.SendMessage(sqsMessage)
	if err == nil {
		log.WithFields(
			log.Fields{
				"awsMessageID": r.MessageId,
				"message":      bodyStr,
				"queueURL":     m.queueURL,
			}).
			Info("enqueued message to sqs")
	}
	return err
}

func (m *messageSink) Status() (*pubsub.Status, error) {
	panic("not implemented")
}

// NewMessageSink constructs an SQS message sink
func NewMessageSink(config *messageSinkConfig) (pubsub.MessageSink, error) {
	awsSession, err := session.NewSession(
		aws.NewConfig().
			WithCredentials(credentials.NewStaticCredentials(config.awsKey, config.awsSecret, "")).
			WithRegion(config.awsRegion).
			WithMaxRetries(config.maxRetries))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize aws session %+v", err)
	}
	s := sqs.New(awsSession)
	return &messageSink{sqs: s}, nil
}
