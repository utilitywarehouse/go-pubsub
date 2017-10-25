package backoff

import (
	"math"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
)

// New returns a new ExponentialBackOffRetryingErrorHandler
func New(handler pubsub.ConsumerMessageHandler, retries int) pubsub.ConsumerErrorHandler {
	return NewWithFallback(
		handler,
		func(msg pubsub.ConsumerMessage, err error) error {
			return err
		},
		retries,
	)
}

// NewWithFallback returns a new ExponentialBackOffRetryingErrorHandler with fallback
func NewWithFallback(handler pubsub.ConsumerMessageHandler, errHandler pubsub.ConsumerErrorHandler, retries int) pubsub.ConsumerErrorHandler {
	return func(msg pubsub.ConsumerMessage, err error) error {
		for i := 0; i < retries; i++ {
			err := handler(msg)
			if err == nil {
				return nil
			}
			<-time.After(time.Duration(3*math.Pow(2, float64(i+1))) * time.Second)
		}
		return errHandler(msg, err)
	}
}
