package example

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/utilitywarehouse/go-pubsub/sqs"
)

func sinkExample() {
	// create a queue first
	creds := credentials.NewStaticCredentials("accessKey", "secretKey", "")
	p := session.Must(session.NewSession())
	cfg := aws.NewConfig().WithCredentials(creds).WithRegion("eu-west-1")

	queue := sqs.NewQueue(p, cfg, "https://sqs.eu-west-1.amazonaws.com/123/queueName")

	// then the sinker
	sink := sqs.NewSink(queue)

	// create your sqs message
	msg := sqs.Message{
		Message: "some payload",
	}

	// and sink it
	if err := sink.PutMessage(&msg); err != nil {
		log.Fatalf("failed to sink in SQS: %v", err)
	}
}
