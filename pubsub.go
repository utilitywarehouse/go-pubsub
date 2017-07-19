package pubsub

import (
	"context"
	"io"
)

// ProducerMessage is an individual message that can be sent
type ProducerMessage interface {
	// Marshal returns the message in wire format
	Marshal() ([]byte, error)
}

// SimpleProducerMessage is a convenience type for simply sending byte slices.
type SimpleProducerMessage []byte

func (sm SimpleProducerMessage) Marshal() ([]byte, error) {
	return []byte(sm), nil
}

type ConsumerMessage struct {
	Data []byte
}

type MessageSink interface {
	io.Closer
	PutMessage(ProducerMessage) error
	Statuser
}

type MessageSource interface {
	// Consume messages will block until error or until the context is done.
	ConsumeMessages(ctx context.Context, handler ConsumerMessageHandler, onError ConsumerErrorHandler) error
	Statuser
}

// Statuser is the interface that wraps the Status method.
type Statuser interface {
	Status() (*Status, error)
}

// Status represents a snapshot of the state of a source or sink.
type Status struct {
	// Working indicates whether the source or sink is in a working state
	Working bool
	// Problems indicates and problems with the source or sink, whether or not they prevent it working.
	Problems []string
}

// ConsumerMessageHandler processes messages, and should return an error if it
// is unable to do so.
type ConsumerMessageHandler func(ConsumerMessage) error

// ConsumerErrorHandler is invoked when a message can not be processed.  If an
// error handler returns an error itself, processing of messages is aborted
type ConsumerErrorHandler func(ConsumerMessage, error) error
