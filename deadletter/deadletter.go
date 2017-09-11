package deadletter

import (
	"encoding/base64"
	"encoding/json"
	"github.com/utilitywarehouse/go-pubsub"
	"fmt"
)

// DeadLetteringErrorHandler a dead lettering ConsumerErrorHandler
type DeadLetteringErrorHandler pubsub.ConsumerErrorHandler

// FailedConsumerMessage a struct for storing failed consumer messages
type FailedConsumerMessage struct {
	Message       pubsub.ConsumerMessage `json:"message"`
	MessageBase64 string                 `json:"messageBase64"`
	MessageTopic  string                 `json:"messageTopic"`
	Err           error                  `json:"error"`
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
			Message:       msg,
			MessageBase64: base64.StdEncoding.EncodeToString(msg.Data),
			MessageTopic:  messageTopic,
			Err:           err,
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