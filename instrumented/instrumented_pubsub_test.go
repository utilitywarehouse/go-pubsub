package instrumented_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/instrumented"
	"github.com/utilitywarehouse/go-pubsub/mockqueue"
)

func TestInstrumentation(t *testing.T) {
	q := mockqueue.NewMockQueue()

	var source pubsub.MessageSource = q
	var sink pubsub.MessageSink = q

	instrumentedSink := instrumented.NewMessageSink(sink, prometheus.CounterOpts{
		Help: "help_sink",
		Name: "test_sink",
	}, "test_topic")

	go func() {

		for i := 0; i < 10; i++ {
			err := instrumentedSink.PutMessage(pubsub.SimpleProducerMessage([]byte("test")))
			if err != nil {
				t.Fatalf("error publishing message: [%+v]", err)
			}
		}
	}()
	instrumentedSource := instrumented.NewMessageSource(source, prometheus.CounterOpts{
		Help: "help_source",
		Name: "test_source",
	}, "test_topic")
	consumed := make(chan pubsub.ConsumerMessage)
	count := 0
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	handler := func(m pubsub.ConsumerMessage) error {
		consumed <- m
		count++
		if count == 9 {
			cancel()
		}
		return nil
	}
	errH := func(m pubsub.ConsumerMessage, e error) error {
		panic(e)
	}
	go func() {
		err := instrumentedSource.ConsumeMessages(ctx, handler, errH)
		if err != nil {
			panic(err)
		}
		close(consumed)
	}()
	for {
		_, ok := <-consumed
		if !ok {
			break
		}
	}
}
