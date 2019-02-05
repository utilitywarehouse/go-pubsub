package instrumented

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// ConcurrentMessageSource is an an Instrumented pubsub MessageSource
// The counter vector will have the labels "status" and "topic"
type ConcurrentMessageSource struct {
	impl    pubsub.ConcurrentMessageSource
	counter *prometheus.CounterVec
	topic   string
}


// NewDefaultConcurrentMessageSource returns a new pubsub MessageSource wrapped in default instrumentation
func NewDefaultConcurrentMessageSource(source pubsub.ConcurrentMessageSource, topic string) pubsub.ConcurrentMessageSource {
	return NewConcurrentMessageSource(
		source,
		prometheus.CounterOpts{
			Name: "messages_consumed_total",
			Help: "The total count of messages consumed",
		},
		topic,
	)
}

// NewConcurrentMessageSource returns a new MessageSource
func NewConcurrentMessageSource(
	source pubsub.ConcurrentMessageSource,
	counterOpts prometheus.CounterOpts,
	topic string) pubsub.ConcurrentMessageSource {
	counter := prometheus.NewCounterVec(counterOpts, []string{"status", "topic"})
	if err := prometheus.Register(counter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}
	counter.WithLabelValues("error", topic).Add(0)
	counter.WithLabelValues("success", topic).Add(0)

	return &ConcurrentMessageSource{source, counter, topic}
}


// ConsumeMessages is an implementation of interface method, wrapping the call in instrumentation
func (ims *ConcurrentMessageSource) ConsumeMessages(
	ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	instrumentedHandler := newMsgHandler(handler, ims.counter, ims.topic)
	return ims.impl.ConsumeMessages(ctx, instrumentedHandler, onError)
}

// ConsumeMessagesConcurrently is an implementation of interface method, wrapping the call in instrumentation
func (ims *ConcurrentMessageSource) ConsumeMessagesConcurrently(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	instrumentedHandler := newMsgHandler(handler, ims.counter, ims.topic)
	return ims.impl.ConsumeMessagesConcurrently(ctx, instrumentedHandler, onError)
}


// Status returns the status of this source, or an error if the status could not be determined.
func (ims *ConcurrentMessageSource) Status() (*pubsub.Status, error) {
	return ims.impl.Status()
}
