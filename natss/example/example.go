package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	nats "github.com/utilitywarehouse/go-pubsub/natss"
)

func main() {

	produce()

	cons, err := nats.NewMessageSource("nats://localhost:4222", "cluster-id", "consumer-02", "demo-topic")
	if err != nil {
		panic(err)
		log.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

	handler := func(m pubsub.ConsumerMessage) error {
		fmt.Printf("message is: %s\n", m.Data)
		return nil
	}

	onError := func(m pubsub.ConsumerMessage, e error) error {
		panic("unexpected error")
	}

	if err := cons.ConsumeMessages(ctx, handler, onError); err != nil {
		log.Fatal(err)
	}
}

func produce() {
	sink, err := nats.NewMessageSink("cluster-id", "demo-topic", "consumer-01", "nats://localhost:4222")
	if err != nil {
		panic(err)
		log.Fatal(err)
	}

	if err := sink.PutMessage(pubsub.SimpleProducerMessage([]byte(fmt.Sprintf("hello. it is currently %v", time.Now())))); err != nil {
		log.Fatal(err)
	}

	if err := sink.Close(); err != nil {
		log.Fatal(err)
	}
}
