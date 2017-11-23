package example

import (
	"context"
	"log"

	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/sqs"
)

func consumerExample() {
	source, err := sqs.NewMessageSource(sqs.MessageSourceConfig{
		Client:   getClient(), // defined in sink.go
		QueueURL: "https://sqs.eu-west-1.amazonaws.com/123/queueName",
		// Optionally set how long you want to wait between API poll requests (in seconds).
		// Defaults to 0 (disabled) if not set.
		WaitSeconds: 1,
	})

	if err != nil {
		log.Fatalf("failed to get SQS source: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Poll for messages. To stop the loop, call `cancel()` and it will cancel the context.
	if err := source.ConsumeMessages(ctx, msgHandler, errHandler); err != nil {
		log.Fatalf("failed to consume from SQS: %v", err)
	}
}

// msgHandler will process messages from SQS.
func msgHandler(msg pubsub.ConsumerMessage) error {
	// Add your message handling logic here.

	return nil
}

// errHandler will be called if message handler fails.
func errHandler(pubsub.ConsumerMessage, error) error {
	// Add your error handling logic here.

	return nil
}
