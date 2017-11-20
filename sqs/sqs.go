package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// QueueSource interface describes the consumer operations.
type QueueSource interface {
	ReceiveMessage() ([]*sqs.Message, error)
	DeleteMessage(receiptHandle *string) error
}

// QueueSink describes message publishing method.
type QueueSink interface {
	SendMessage(payload *string) error
}

// SourceQueue holds dependencies to poll for messages.
type SourceQueue struct {
	queueURL string // AWS queue URL
	sqs      *sqs.SQS
	input    *sqs.ReceiveMessageInput
}

// SinkQueue holds dependencies to sink messages.
type SinkQueue struct {
	queueURL string // AWS queue URL
	sqs      *sqs.SQS
	input    *sqs.SendMessageInput
}

// NewConsumerQueue returns a new SQS consumer
func NewConsumerQueue(awsAccessKey, awsSecretKey, awsRegion, queueURL string) *SourceQueue {
	creds := credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, "")
	sqsInstance := sqs.New(session.Must(session.NewSession()), aws.NewConfig().WithCredentials(creds).WithRegion(awsRegion))

	return &SourceQueue{
		queueURL: queueURL,
		sqs:      sqsInstance,
		input: &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			AttributeNames:      aws.StringSlice([]string{"All"}),
			MaxNumberOfMessages: aws.Int64(10),
		},
	}
}

// ReceiveMessage returns a []*sqs.Message or an error if the operation fails.
func (s *SourceQueue) ReceiveMessage() ([]*sqs.Message, error) {
	output, err := s.sqs.ReceiveMessage(s.input)
	if err != nil {
		return nil, err
	}

	return output.Messages, nil
}

// DeleteMessage uses the provided receiptHandle identifier (returned after consuming a message)
// and makes a delete request to AWS SQS.
func (s *SourceQueue) DeleteMessage(receiptHandle *string) error {
	_, err := s.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.queueURL),
		ReceiptHandle: receiptHandle,
	})

	return err
}

// NewSinkQueue returns a pointer to a new Sink instance.
func NewSinkQueue(awsAccessKey, awsSecretKey, awsRegion, queueURL string) QueueSink {
	creds := credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, "")
	sqsInstance := sqs.New(session.Must(session.NewSession()), aws.NewConfig().WithCredentials(creds).WithRegion(awsRegion))

	return &SinkQueue{
		queueURL: queueURL,
		sqs:      sqsInstance,
		input:    &sqs.SendMessageInput{QueueUrl: &queueURL},
	}
}

// SendMessage sinks message in SQS.
func (s *SinkQueue) SendMessage(payload *string) error {
	input := s.input
	input.MessageBody = payload

	_, err := s.sqs.SendMessage(input)

	return err
}
