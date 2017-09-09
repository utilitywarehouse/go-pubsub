package deadletter

import (
	"encoding/base64"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/utilitywarehouse/go-pubsub"
)

type DeadLetteringErrorHandler pubsub.ConsumerErrorHandler

type FailedConsumerMessage struct {
	Message       pubsub.ConsumerMessage `json:"message"`
	MessageBase64 string                 `json:"messageBase64"`
	Err           error                  `json:"error"`
}

func New(sink pubsub.MessageSink) DeadLetteringErrorHandler {
	return func(msg pubsub.ConsumerMessage, err error) error {
		failedMsg := FailedConsumerMessage{
			Message:       msg,
			MessageBase64: base64.StdEncoding.EncodeToString(msg.Data),
			Err:           err,
		}
		failedMsgJSON, err := json.Marshal(failedMsg)
		if err != nil {
			return errors.Wrap(err, "Error serialising failed message to JSON")
		}
		if err := sink.PutMessage(pubsub.SimpleProducerMessage(failedMsgJSON)); err != nil {
			return errors.Wrap(err, "Error producing failed message to dead letter queue")
		}
		return nil
	}
}

func NewWithFallback(sink pubsub.MessageSink, errHandler pubsub.ConsumerErrorHandler) DeadLetteringErrorHandler {
	return func(msg pubsub.ConsumerMessage, err error) error {
		failedMsg := FailedConsumerMessage{
			Message:       msg,
			MessageBase64: base64.StdEncoding.EncodeToString(msg.Data),
			Err:           err,
		}
		failedMsgJSON, err := json.Marshal(failedMsg)
		if err != nil {
			return errHandler(msg, errors.Wrap(err, "Error serialising failed message to JSON"))
		}
		if err := sink.PutMessage(pubsub.SimpleProducerMessage(failedMsgJSON)); err != nil {
			return errHandler(msg, errors.Wrap(err, "Error producing failed message to dead letter queue")
		}
		return nil
	}
}