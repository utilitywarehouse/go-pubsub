package instrumented

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// MessageSource is an an Instrumented pubsub MessageSource
// The counter vector will have the labels "status" and "topic"
type MessageSource struct {
	impl    pubsub.MessageSource
	counter *prometheus.CounterVec
	topic   string
}

// NewDefaultMessageSource returns a new pubsub MessageSource wrapped in default instrumentation
func NewDefaultMessageSource(source pubsub.MessageSource, topic string) pubsub.MessageSource {
	return NewMessageSource(
		source,
		prometheus.CounterOpts{
			Name: "messages_consumed_total",
			Help: "The total count of messages consumed",
		},
		topic,
	)
}

// NewMessageSource returns a new MessageSource
func NewMessageSource(
	source pubsub.MessageSource,
	counterOpts prometheus.CounterOpts,
	topic string) pubsub.MessageSource {
	counter := prometheus.NewCounterVec(counterOpts, []string{"status", "topic"})
	if err := prometheus.Register(counter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}
	return &MessageSource{source, counter, topic}
}

func newMsgHandler(
	handler func(msg pubsub.ConsumerMessage) error,
	vec *prometheus.CounterVec, topic string) func(msg pubsub.ConsumerMessage) error {

	return func(msg pubsub.ConsumerMessage) error {
		if err := handler(msg); err != nil {
			vec.WithLabelValues("error", topic).Inc()
			return err
		}
		vec.WithLabelValues("success", topic).Inc()
		return nil
	}
}

// ConsumeMessages is an implementation of interface method, wrapping the call in instrumentation
func (ims *MessageSource) ConsumeMessages(
	ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	instrumentedHandler := newMsgHandler(handler, ims.counter, ims.topic)
	return ims.impl.ConsumeMessages(ctx, instrumentedHandler, onError)
}

// Status returns the status of this source, or an error if the status could not be determined.
func (ims *MessageSource) Status() (*pubsub.Status, error) {
	return ims.impl.Status()
}

// MessageSink is an instrumented implementation of the pubsub MessageSink
type MessageSink struct {
	impl    pubsub.MessageSink
	produce func(pubsub.ProducerMessage) error
}

// PutMessage implements pubsub MessageSink interface method wrapped in instrumentation
func (ims *MessageSink) PutMessage(m pubsub.ProducerMessage) error {
	return ims.produce(m)
}

// Close closes the message sink
func (ims *MessageSink) Close() error {
	return ims.impl.Close()
}

// Status returns the status of this sink, or an error if the status could not be determined.
func (ims *MessageSink) Status() (*pubsub.Status, error) {
	return ims.impl.Status()
}

// NewDefaultMessageSink returns a new pubsub MessageSink wrapped in default instrumentation
func NewDefaultMessageSink(sink pubsub.MessageSink, topic string) pubsub.MessageSink {
	return NewMessageSink(
		sink,
		prometheus.CounterOpts{
			Name: "messages_produced_total",
			Help: "The total count of messages produced",
		},
		topic,
	)
}

// NewMessageSink constructs a new pubsub MessageSink wrapped in instrumentation
// The counter vector will have the labels status and topic
func NewMessageSink(sink pubsub.MessageSink, counterOpts prometheus.CounterOpts, topic string) pubsub.MessageSink {
	sinkCounter := prometheus.NewCounterVec(counterOpts, []string{"status", "topic"})
	if err := prometheus.Register(sinkCounter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			sinkCounter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}
	produceMessage := func(m pubsub.ProducerMessage) error {
		err := sink.PutMessage(m)
		if err != nil {
			sinkCounter.WithLabelValues("error", topic).Add(1)
		} else {
			sinkCounter.WithLabelValues("success", topic).Add(1)
		}
		return err
	}
	return &MessageSink{
		impl:    sink,
		produce: produceMessage,
	}
}
