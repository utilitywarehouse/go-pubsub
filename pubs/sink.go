package pubs

import (
	"fmt"
	"net/url"
	"strings"

	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/kafka"
	"github.com/utilitywarehouse/go-pubsub/natss"
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

func newKafkaSink(uri *url.URL) (pubsub.MessageSink, error) {
	q := uri.Query()

	topic := strings.Trim(uri.Path, "/")

	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	conf := kafka.MessageSinkConfig{
		Brokers: []string{uri.Host},
		Topic:   topic,
	}

	conf.Brokers = append(conf.Brokers, q["broker"]...)

	return kafkaSinker(conf)
}

var kafkaSinker = kafka.NewMessageSink

func newNatsStreamingSink(uri *url.URL) (pubsub.MessageSink, error) {
	q := uri.Query()

	natsURL := "nats://" + uri.Host

	topic := strings.Trim(uri.Path, "/")
	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	return natsStreamingSinker(natsURL, q.Get("cluster-id"), q.Get("client-id"), topic)

}

var natsStreamingSinker = natss.NewMessageSink
