package instrumented

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// InstrumentedMessageSource is an an Instrumented pubsub MessageSource
type InstrumentedMessageSource struct {
	impl    pubsub.MessageSource
	counter *prometheus.CounterVec
	topic   string
}

// NewInstrumentedMessageSource returns a new InstrumentedMessageSource
func NewInstrumentedMessageSource(
	source pubsub.MessageSource,
	counterOpts prometheus.CounterOpts,
	topic string) pubsub.MessageSource {
	counter := prometheus.NewCounterVec(counterOpts, []string{"status", topic})
	prometheus.MustRegister(counter)
	return &InstrumentedMessageSource{source, counter, topic}
}

func newInstrumentedMsgHandler(
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
func (ims *InstrumentedMessageSource) ConsumeMessages(
	ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {
	instrumentedHandler := newInstrumentedMsgHandler(handler, ims.counter, ims.topic)
	return ims.impl.ConsumeMessages(ctx, instrumentedHandler, onError)
}

// InstrumentedMessageSink is an instrumented implementation of the pubsub MessageSink
type InstrumentedMessageSink struct {
	impl    pubsub.MessageSink
	produce func(pubsub.ProducerMessage) error
}

// PutMessage implements pubsub MessageSink interface method wrapped in instrumentation
func (ims *InstrumentedMessageSink) PutMessage(m pubsub.ProducerMessage) error {
	return ims.produce(m)
}

// Close closes the message sink
func (ims *InstrumentedMessageSink) Close() error {
	return ims.Close()
}

// NewInstrumentedMessageSink constructs a new pubsub MessageSink wrapped in instrumentation
func NewInstrumentedMessageSink(sink pubsub.MessageSink, counterOpts prometheus.CounterOpts, topic string) pubsub.MessageSink {
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
	return &InstrumentedMessageSink{
		impl:    sink,
		produce: produceMessage,
	}
}
