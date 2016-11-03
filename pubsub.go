package pubsub

import (
	"io"
)

// Message is an individual message that can be produced or consumed.
type Message struct {
	// Data is the raw message data
	Data []byte
}

type MessageSink interface {
	io.Closer
	PutMessage(Message) error
}

type MessageSource interface {
	io.Closer
	// Consume messages will block until the source is closed
	ConsumeMessages(handler MessageHandler, onError ErrorHandler) error
}

// MessageHandler processes messages, and should return an error if it is
// unable to do so.
type MessageHandler func(Message) error

// ErrorHandler is invoked when a message can not be processed.  If an error
// handler returns an error itself, processing of messages is aborted
type ErrorHandler func(Message, error) error
