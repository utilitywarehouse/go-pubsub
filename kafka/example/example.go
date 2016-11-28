package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/kafka"
)

func main() {

	produce()

	cons := kafka.NewMessageSource(kafka.MessageSourceConfig{
		ConsumerGroup: "demo-group",
		Topic:         "demo-topic",
		Zookeepers:    []string{"localhost:2181"},
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

func (m MyMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func produce() {

	sink, err := kafka.NewMessageSink(
		kafka.MessageSinkConfig{
			Topic:   "demo-topic",
			Brokers: []string{"localhost:9092"},
			KeyFunc: func(m pubsub.ProducerMessage) []byte {
				return []byte(m.(MyMessage).CustomerID)
			},
		})
	if err != nil {
		log.Fatal(err)
	}

	sink.PutMessage(MyMessage{
		CustomerID: "customer-01",
		Message:    fmt.Sprintf("hello. it is currently %v", time.Now()),
	})

	sink.Close()
}
