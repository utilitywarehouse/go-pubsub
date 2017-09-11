package deadletter

import (
	"encoding/json"
	"fmt"
	"github.com/utilitywarehouse/go-pubsub"
)

// DeadLetteringErrorHandler a dead lettering ConsumerErrorHandler
type DeadLetteringErrorHandler pubsub.ConsumerErrorHandler

// FailedConsumerMessage a struct for storing failed consumer messages
type FailedConsumerMessage struct {
	Message      []byte `json:"message"`
	MessageTopic string `json:"messageTopic"`
	Err          string `json:"error"`
}

// New returns a new DeadLetteringErrorHandler
func New(sink pubsub.MessageSink, messageTopic string) DeadLetteringErrorHandler {
	return NewWithFallBack(
		sink,
		func(msg pubsub.ConsumerMessage, err error) error {
			return err
		},
		messageTopic,
	)
}

// NewWithFallBack returns a new DeadLetteringErrorHandler with fall back
func NewWithFallBack(sink pubsub.MessageSink, errHandler pubsub.ConsumerErrorHandler, messageTopic string) DeadLetteringErrorHandler {
	return func(msg pubsub.ConsumerMessage, err error) error {
		failedMsg := FailedConsumerMessage{
			Message:      msg.Data,
			MessageTopic: messageTopic,
			Err:          err.Error(),
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
