package main

import (
	"context"
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

	cons := amqp.NewMessageSource(amqp.MessageSourceConfig{
		Address:       "amqp://localhost:5672/",
		ConsumerGroup: "demo-group",
		Topic:         "demo-topic",
	})

	// consume messages for 2 seconds
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

type MyMessage struct {
	CustomerID string
	Message    string
}

func (m MyMessage) MarshalPubSub() ([]byte, error) {
	return json.Marshal(m)
}
