package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type queue interface {
	ReceiveMessage() ([]*sqs.Message, error)
	DeleteMessage(receiptHandle *string) error
	SendMessage(payload *string) error
}

type sqsQueue struct {
	client       *sqs.SQS
	receiveInput *sqs.ReceiveMessageInput
	sendInput    *sqs.SendMessageInput
	deleteInput  *sqs.DeleteMessageInput
}

// NewQueue returns a new SQS queue
func NewQueue(p client.ConfigProvider, config *aws.Config, queueURL string) queue {
	return &sqsQueue{
		client: sqs.New(p, config),
		receiveInput: &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			AttributeNames:      aws.StringSlice([]string{"All"}),
			MaxNumberOfMessages: aws.Int64(1),
		},
		sendInput:   &sqs.SendMessageInput{QueueUrl: &queueURL},
		deleteInput: &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL)},
	}
}

// ReceiveMessage returns a []*sqs.Message or an error if the operation fails.
func (s *sqsQueue) ReceiveMessage() ([]*sqs.Message, error) {
	output, err := s.client.ReceiveMessage(s.receiveInput)
	if err != nil {
		return nil, err
	}

	return output.Messages, nil
}

// DeleteMessage uses the provided receiptHandle identifier (returned after consuming a message)
// and makes a delete request to AWS SQS.
func (s *sqsQueue) DeleteMessage(receiptHandle *string) error {
	input := s.deleteInput
	input.ReceiptHandle = receiptHandle

	_, err := s.client.DeleteMessage(input)

	return err
}

// SendMessage sinks message in SQS.
func (s *sqsQueue) SendMessage(payload *string) error {
	input := s.sendInput
	input.MessageBody = payload

	_, err := s.client.SendMessage(input)

	return err
}
