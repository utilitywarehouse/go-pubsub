package instrumented

import (
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*syncReconnectSink)(nil)

// ReconnectionOptions represents the options
// to configure and hook up to reconnection events
type ReconnectionOptions struct {
	ReconnectSynchronously bool
	OnReconnectFailed      func(err error)
	OnReconnectSuccess     func(sink pubsub.MessageSink, err error)
}

// NewReconnectorSink creates a new reconnector sink
func NewReconnectorSink(
	newSink func() (pubsub.MessageSink, error),
	options *ReconnectionOptions,
) (pubsub.MessageSink, error) {

	if options.ReconnectSynchronously {
		return newSyncReconnectorSink(newSink, options)
	} else {
		return newAsyncReconnectorSink(newSink, options)
	}
}
