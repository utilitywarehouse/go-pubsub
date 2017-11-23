package example

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsSQS "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/utilitywarehouse/go-pubsub/sqs"
)

// This is an example struct that satisfies pubsub.ProducerMessage interface.
// You'd have to create a struct in your application that satisfies this interface.
type myMessage struct {
	data string
}

// setting up SQS client is the same for both sink and source
func getClient() *awsSQS.SQS {
	creds := credentials.NewStaticCredentials("accessKey", "secretKey", "")
	p := session.Must(session.NewSession())
	cfg := aws.NewConfig().WithCredentials(creds).WithRegion("eu-west-1")

	return awsSQS.New(p, cfg)
}

func sinkExample() {
	queueURL := "https://sqs.eu-west-1.amazonaws.com/123/queueName"

	// create message sink passing AWS SQS client
	sink, err := sqs.NewMessageSink(sqs.MessageSinkConfig{
		Client:   getClient(),
		QueueURL: &queueURL,
	})

	if err != nil {
		log.Panicf("failed to create sqs sink: %v", err)
	}

	// Although this doesn't close an actual connection, it is available in the API and
	// can act as a safeguard to prevent you from writing messages to a closed SQS sink.
	// NOTE: We use panic in this example and not Fatal to ensure defer is called.
	defer sink.Close()

	// Create your sqs message (struct must satisfy pubsub.ProducerMessage interface).
	msg := myMessage{data: "some payload"}

	// and sink it
	if err := sink.PutMessage(&msg); err != nil {
		log.Panicf("failed to sink in SQS: %v", err)
	}
}

func (m *myMessage) Marshal() ([]byte, error) {
	return []byte(m.data), nil
}
