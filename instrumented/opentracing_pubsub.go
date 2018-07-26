package instrumented

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/telecom-contracts/go/proto"
)

type TracedMessageSink interface {
	ProduceEvent(ctx context.Context, envelope *contracts.Envelope) error
}

type tracedMessageSink struct {
	tracer opentracing.Tracer
	sink   pubsub.MessageSink
}

func (s *tracedMessageSink) ProduceEvent(ctx context.Context, envelope *contracts.Envelope) error {
	var sp opentracing.Span
	defer sp.Finish()
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		sp = opentracing.StartSpan(
			fmt.Sprintf("publish event %s", envelope.Payload.TypeUrl),
			opentracing.ChildOf(parentSpan.Context()),
		)
	} else {
		sp = opentracing.StartSpan(
			fmt.Sprintf("event_%s_publication", envelope.Payload.TypeUrl),
			sp.Context().ForeachBaggageItem()
			)
	}
	sp.LogFields(
		log.String("payload.typeURL", envelope.Payload.TypeUrl),
		log.String("type", "SERVER"),
	)

	envelope.Metadata = map[string]string{

	}

	p, err := proto.Marshal(envelope)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event envelope")
	}

	return s.sink.PutMessage(pubsub.SimpleProducerMessage(p))
}


func NewTracedMessageSink(sink pubsub.MessageSink, tracer opentracing.Tracer) TracedMessageSink {
	return &tracedMessageSink{
		sink:   sink,
		tracer: tracer,
	}
}
