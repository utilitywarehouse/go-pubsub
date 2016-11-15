package main

import (
	"fmt"
	"log"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	nats "github.com/utilitywarehouse/go-pubsub/nats"
)

func main() {

	cons, err := nats.NewNatsMessageSource("demo-topic", "nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}

	// consume messages
	go func() {

		handler := func(m pubsub.ConsumerMessage) error {
			fmt.Printf("message is: %s\n", m.Data)
			return nil
		}

		onError := func(m pubsub.ConsumerMessage, e error) error {
			panic("unexpected error")
		}

		if err := cons.ConsumeMessages(handler, onError); err != nil {
			log.Fatal(err)
		}
	}()

	produce()

	// consume for 2 seconds, then close
	time.Sleep(2 * time.Second)

	if err := cons.Close(); err != nil {
		log.Fatal(err)
	}

}

func produce() {
	sink, err := nats.NewNatsMessageSink("demo-topic", "nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}

	if err := sink.PutMessage(pubsub.SimpleProducerMessage([]byte(fmt.Sprintf("hello. it is currently %v", time.Now())))); err != nil {
		log.Fatal(err)
	}

	if err := sink.Close(); err != nil {
		log.Fatal(err)
	}
}
