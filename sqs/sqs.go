package sqs

import awsSQS "github.com/aws/aws-sdk-go/service/sqs"

// Queue interface is used in order to be able to mock SQS client in unit tests and has
// the same signature as the methods in aws-sdk-go. Interface holds only the messages
// we're interested in pubsub.
type Queue interface {
	ReceiveMessage(*awsSQS.ReceiveMessageInput) (*awsSQS.ReceiveMessageOutput, error)
	DeleteMessage(*awsSQS.DeleteMessageInput) (*awsSQS.DeleteMessageOutput, error)
	SendMessage(*awsSQS.SendMessageInput) (*awsSQS.SendMessageOutput, error)
}
