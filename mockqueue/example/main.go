package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/mockqueue"
)

func main() {

	q := mockqueue.NewMockQueue()

	// MockQueue implements both source and sink
	var cons pubsub.MessageSource = q
	var sink pubsub.MessageSink = q

	go func() {
		tick := time.NewTicker(500 * time.Millisecond)
		for {
			<-tick.C
			sink.PutMessage(pubsub.SimpleProducerMessage([]byte("hello")))
		}
	}()

	onError := func(m pubsub.ConsumerMessage, err error) error {
		panic("unexpected error")
	}
	handler := func(m pubsub.ConsumerMessage) error {
		fmt.Println(string(m.Data))
		return nil
	}

	ctx, _ := context.WithTimeout(context.Background(), 750*time.Millisecond)

	if err := cons.ConsumeMessages(ctx, handler, onError); err != nil {
		log.Fatal(err)
	}

}
