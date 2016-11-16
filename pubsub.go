package pubsub

import (
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
}

type MessageSource interface {
	io.Closer
	// Consume messages will block until the source is closed
	ConsumeMessages(handler ConsumerMessageHandler, onError ConsumerErrorHandler) error
}

// ConsumerMessageHandler processes messages, and should return an error if it
// is unable to do so.
type ConsumerMessageHandler func(ConsumerMessage) error

// ConsumerErrorHandler is invoked when a message can not be processed.  If an
// error handler returns an error itself, processing of messages is aborted
type ConsumerErrorHandler func(ConsumerMessage, error) error
