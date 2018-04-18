package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
	psstraw "github.com/utilitywarehouse/go-pubsub/straw"
	"github.com/uw-labs/straw"
)

func main() {

	produce()

	cons := psstraw.NewMessageSource(&straw.OsStreamStore{}, psstraw.MessageSourceConfig{Path: "/tmp/"})

	// consume messages for 2 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

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

	sink, err := psstraw.NewMessageSink(
		&straw.OsStreamStore{},
		psstraw.MessageSinkConfig{"/tmp/"},
	)
	if err != nil {
		log.Fatal(err)
	}

	sink.PutMessage(MyMessage{
		CustomerID: "customer-01",
		Message:    fmt.Sprintf("hello. it is currently %v", time.Now()),
	})

	sink.Close()
}
