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

// NewMessageSource returns a new MessageSource
func NewMessageSource(
	source pubsub.MessageSource,
	counterOpts prometheus.CounterOpts,
	topic string) pubsub.MessageSource {
	counter := prometheus.NewCounterVec(counterOpts, []string{"status", "topic"})
	prometheus.MustRegister(counter)
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

// NewMessageSink constructs a new pubsub MessageSink wrapped in instrumentation
// The counter vector will have the labels status and topic
func NewMessageSink(sink pubsub.MessageSink, counterOpts prometheus.CounterOpts, topic string) pubsub.MessageSink {
	sinkCounter := prometheus.NewCounterVec(counterOpts, []string{"status", "topic"})
	prometheus.MustRegister(sinkCounter)
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
