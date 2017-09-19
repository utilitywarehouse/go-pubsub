package pubs

import (
	"fmt"
	"net/url"

	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// NewSink will return a message sink based on the supplied URL.
// Examples:
//  kafka://localhost:123/my-topic/?metadata-refresh=2s&broker=localhost:234&broker=localhost:345
//  nats-streaming://localhost:123/my-topic?cluster-id=cid-1&consumer-id=cons-1
func NewSink(uri string) (pubsub.MessageSink, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	switch parsed.Scheme {
	case "kafka":
		return newKafkaSink(parsed)
	case "nats-streaming":
		return newNatsStreamingSink(parsed)
	default:
		return nil, fmt.Errorf("unknown scheme : %s", parsed.Scheme)
	}
}
