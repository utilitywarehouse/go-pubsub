package pubs

import (
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/kafka"
	"github.com/utilitywarehouse/go-pubsub/natss"
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
		return newKafka(parsed)
	case "nats-streaming":
		return newNatsStreaming(parsed)
	default:
		return nil, fmt.Errorf("unknown scheme : %s", parsed.Scheme)
	}
}

func newKafka(uri *url.URL) (pubsub.MessageSource, error) {
	q := uri.Query()

	topic := strings.Trim(uri.Path, "/")

	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	conf := kafka.MessageSourceConfig{
		Brokers:       []string{uri.Host},
		ConsumerGroup: q.Get("consumer-group"),
		Topic:         topic,
	}

	conf.Brokers = append(conf.Brokers, q["broker"]...)

	switch q.Get("offset") {
	case "latest":
		conf.Offset = kafka.OffsetLatest
	case "oldest":
		conf.Offset = kafka.OffsetLatest
	case "":
	default:
		log.Printf("ignoring unknown offset value '%s'\n", q.Get("offset"))
	}

	dur := q.Get("metadata-refresh")
	if dur != "" {
		d, err := time.ParseDuration(dur)
		if err != nil {
			return nil, fmt.Errorf("failed to parse refresh duration : %v", err)
		}
		conf.MetadataRefreshFrequency = d
	}

	return kafkaSourcer(conf), nil
}

var kafkaSourcer = kafka.NewMessageSource

func newNatsStreaming(uri *url.URL) (pubsub.MessageSource, error) {
	q := uri.Query()

	natsURL := "nats://" + uri.Host

	topic := strings.Trim(uri.Path, "/")
	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	return natsStreamingSourcer(natss.MessageSourceConfig{
		NatsURL:    natsURL,
		ClusterID:  q.Get("cluster-id"),
		ConsumerID: q.Get("consumer-id"),
		Topic:      topic,
	})

}

var natsStreamingSourcer = natss.NewMessageSource
