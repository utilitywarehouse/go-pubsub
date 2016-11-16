package main

import (
	"encoding/json"
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

func produce() {

	keyFunc := func(m pubsub.ProducerMessage) []byte {
		return []byte(m.(MyMessage).CustomerID)
	}

	sink, err := kafka.NewMessageSink("demo-topic", []string{"localhost:9092"}, keyFunc)
	if err != nil {
		log.Fatal(err)
	}

	sink.PutMessage(MyMessage{
		CustomerID: "customer-01",
		Message:    fmt.Sprintf("hello. it is currently %v", time.Now()),
	})

	sink.Close()
}
