package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/amqp"
)

func main() {

	sink, err := amqp.NewMessageSink(amqp.MessageSinkConfig{
		Address: "amqp://localhost:5672/",
		Topic:   "demo-topic",
	})

	if err != nil {
		log.Fatal(err)
	}

	sink.PutMessage(MyMessage{
		CustomerID: "customer-01",
		Message:    fmt.Sprintf("hello. it is currently %v", time.Now()),
	})

	defer sink.Close()

	cons := amqp.NewMessageSource(amqp.MessageSourceConfig{
		Address:       "amqp://localhost:5672/",
		ConsumerGroup: "demo-group",
		Topic:         "demo-topic",
	})

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

	// consume for 2 seconds, then close
	time.Sleep(2 * time.Second)

	if err := cons.Close(); err != nil {
		log.Fatal(err)
	}

}

type MyMessage struct {
	CustomerID string
	Message    string
}

func (m MyMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}
