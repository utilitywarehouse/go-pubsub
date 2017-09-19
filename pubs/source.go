package pubs

import (
	"fmt"
	"net/url"

	pubsub "github.com/utilitywarehouse/go-pubsub"
)

// NewSource will return a message source based on the supplied URL.
// Examples:
//  kafka://localhost:123/my-topic/?offset=latest&consumer-group=g1&metadata-refresh=2s&broker=localhost:234&broker=localhost:345
//  nats-streaming://localhost:123/my-topic?cluster-id=cid-1&consumer-id=cons-1
func NewSource(uri string) (pubsub.MessageSource, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	switch parsed.Scheme {
	case "kafka":
		return newKafkaSource(parsed)
	case "nats-streaming":
		return newNatsStreamingSource(parsed)
	default:
		return nil, fmt.Errorf("unknown scheme : %s", parsed.Scheme)
	}
}
