package main

import (
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
		tick := time.NewTicker(1 * time.Second)
		for {
			<-tick.C
			sink.PutMessage(pubsub.SimpleProducerMessage([]byte("hello")))
		}
	}()

	go func() {
		onError := func(m pubsub.ConsumerMessage, err error) error {
			panic("unexpected error")
		}
		handler := func(m pubsub.ConsumerMessage) error {
			fmt.Println(string(m.Data))
			return nil
		}

		if err := cons.ConsumeMessages(handler, onError); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(5 * time.Second)

	if err := q.Close(); err != nil {
		log.Fatal(err)
	}

}
