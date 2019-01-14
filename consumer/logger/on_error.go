package logger

import (
	"github.com/utilitywarehouse/go-pubsub"
)

// Logger allows for logging errors to any destination
type Logger interface {
	Error(args ...interface{})
}

// NewErrorHandler returns a new error handler that calls Error() on the Logger interface
// If a error handler fallback is specified, it will be called after logging, propagating any returned error
func NewErrorHandler(logger Logger, onErrorFallback pubsub.ConsumerErrorHandler) pubsub.ConsumerErrorHandler {
	return func(msg pubsub.ConsumerMessage, err error) error {
		logger.Error(err)

		if onErrorFallback != nil {
			return onErrorFallback(msg, err)
		}

		return nil
	}

}
