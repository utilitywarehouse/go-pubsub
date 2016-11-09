package main

import (
	"fmt"
	"log"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/kafka"
)

func main() {

	produce()

	cons := kafka.NewMessageSource("demo-group", "demo-topic", []string{"localhost:2181"})

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
	time.Sleep(2 * time.Second)

	if err := cons.Close(); err != nil {
		log.Fatal(err)
	}

}

func produce() {
	sink, err := kafka.NewMessageSink("demo-topic", []string{"localhost:9092"})
	if err != nil {
		log.Fatal(err)
	}

	sink.PutMessage(pubsub.Message{Data: []byte(fmt.Sprintf("hello. it is currently %v", time.Now()))})

	sink.Close()
}
