package sqs

import awsSQS "github.com/aws/aws-sdk-go/service/sqs"

// Queue interface has the same signature as the methods in SQS struct in aws-sdk-go.
// Holds only the messages we're interested in pubsub.
type Queue interface {
	ReceiveMessage(*awsSQS.ReceiveMessageInput) (*awsSQS.ReceiveMessageOutput, error)
	DeleteMessage(*awsSQS.DeleteMessageInput) (*awsSQS.DeleteMessageOutput, error)
	SendMessage(*awsSQS.SendMessageInput) (*awsSQS.SendMessageOutput, error)
}
