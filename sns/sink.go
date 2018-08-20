package sns

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/pkg/errors"
	"github.com/utilitywarehouse/go-pubsub"
)

type messageSink struct {
	client    snsiface.SNSAPI
	topic     string
	lastError error
}

// NewSNSSink is a constructor for new AWS SNS MessageSink type
func NewSNSSink(conf *aws.Config, topic string) (pubsub.MessageSink, error) {
	sess, err := session.NewSession(conf)
	if err != nil {
		return nil, errors.Wrap(err, "could not construct AWS Session")
	}

	return &messageSink{
		client:    sns.New(sess),
		topic:     topic,
		lastError: nil,
	}, nil
}

// PutMessage sends ProducerMessage types to AWS SNS
func (s *messageSink) PutMessage(message pubsub.ProducerMessage) error {
	b, err := message.Marshal()
	if err != nil {
		s.lastError = err
		return errors.Wrap(err, "SNS Sink could not marshal ProducerMessage")
	}

	input := &sns.PublishInput{
		Message:  aws.String(string(b)),
		TopicArn: &s.topic,
	}

	_, err = s.client.Publish(input)
	if err != nil {
		s.lastError = err
		return errors.Wrap(err, "error publishing to SNS")
	}

	return nil
}

// Status used to check status of connection to AWS SNS
func (s *messageSink) Status() (*pubsub.Status, error) {
	status := &pubsub.Status{
		Working: true,
	}
	if s.lastError != nil {
		status.Working = false
		status.Problems = append(status.Problems, s.lastError.Error())
	}

	return status, nil
}

// Close is stubbed as there is no equivalent on the SNS Client.
func (s *messageSink) Close() error {
	return nil
}
