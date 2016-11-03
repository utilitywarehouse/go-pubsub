package main

import (
	"fmt"
	"log"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	nats "github.com/utilitywarehouse/go-pubsub/natss"
)

func main() {

	produce()

	cons, err := nats.NewNatsMessageSource("demo-topic", "consumer-02", "nats://localhost:4222")
	if err != nil {
		panic(err)
		log.Fatal(err)
	}

	// consume messages
	go func() {

		handler := func(m pubsub.Message) error {
			fmt.Printf("message is: %s\n", m.Data)
			return nil
		}

		onError := func(m pubsub.Message, e error) error {
			panic("unexpected error")
		}

		if err := cons.ConsumeMessages(handler, onError); err != nil {
			log.Fatal(err)
		}
	}()

	// consume for 2 seconds, then close
	time.Sleep(9 * time.Second)

	if err := cons.Close(); err != nil {
		log.Fatal(err)
	}

}

func produce() {
	sink, err := nats.NewMessageSink("demo-topic", "consumer-01", "nats://localhost:4222")
	if err != nil {
		panic(err)
		log.Fatal(err)
	}

	if err := sink.PutMessage(pubsub.Message{Data: []byte(fmt.Sprintf("hello. it is currently %v", time.Now()))}); err != nil {
		log.Fatal(err)
	}

	if err := sink.Close(); err != nil {
		log.Fatal(err)
	}
}
