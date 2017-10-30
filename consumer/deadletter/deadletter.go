package deadletter

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
)

// FailedConsumerMessage a struct for storing failed consumer messages
type FailedConsumerMessage struct {
	Message      []byte    `json:"message"`
	MessageTopic string    `json:"messageTopic"`
	Err          string    `json:"error"`
	Timestamp    time.Time `json:"timestamp"`
}

// New returns a new ConsumerErrorHandler which produces JSON serialized FailedConsumerMessage to sink
func New(sink pubsub.MessageSink, messageTopic string) pubsub.ConsumerErrorHandler {
	return NewWithFallback(
		sink,
		func(msg pubsub.ConsumerMessage, err error) error {
			return err
		},
		messageTopic,
	)
}

// NewWithFallback returns a new ConsumerErrorHandler which produces JSON serialized FailedConsumerMessage to sink with fallback handler
func NewWithFallback(sink pubsub.MessageSink, errHandler pubsub.ConsumerErrorHandler, messageTopic string) pubsub.ConsumerErrorHandler {
	return func(msg pubsub.ConsumerMessage, err error) error {
		failedMsg := FailedConsumerMessage{
			Message:      msg.Data,
			MessageTopic: messageTopic,
			Err:          err.Error(),
			Timestamp:    time.Now(),
		}
		failedMsgJSON, err := json.Marshal(failedMsg)
		if err != nil {
			return errHandler(msg, fmt.Errorf("Error serialising failed message to JSON: %v", err))
		}
		if err := sink.PutMessage(pubsub.SimpleProducerMessage(failedMsgJSON)); err != nil {
			return errHandler(msg, fmt.Errorf("Error producing failed message to dead letter queue: %v", err))
		}
		return nil
	}
}
