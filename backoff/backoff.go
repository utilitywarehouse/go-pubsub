package backoff

import (
	"time"
	"math"
	"github.com/utilitywarehouse/go-pubsub"
)

type ExponentialBackOffRetryingErrorHandler pubsub.ConsumerErrorHandler

func New(handler pubsub.ConsumerMessageHandler) ExponentialBackOffRetryingErrorHandler {
	return NewWithFallBack(handler, func(msg pubsub.ConsumerMessage, err error) error {
		return err
	})
}

func NewWithFallBack(handler pubsub.ConsumerMessageHandler, errHandler pubsub.ConsumerErrorHandler) ExponentialBackOffRetryingErrorHandler {
	return func(msg pubsub.ConsumerMessage, err error) error {
		for i := 0; i < 3; i++ {
			err := handler(msg)
			if err == nil {
				return nil
			}
			<-time.After(time.Duration(3*math.Pow(2, float64(i+1))) * time.Second)
		}
		return errHandler(msg, err)
	}
}