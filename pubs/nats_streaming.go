package pubs

import (
	"fmt"
	"net/url"
	"strings"

	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/natss"
)

func newNatsStreamingSink(uri *url.URL) (pubsub.MessageSink, error) {
	q := uri.Query()

	natsURL := "nats://" + uri.Host

	topic := strings.Trim(uri.Path, "/")
	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	return natsStreamingSinker(natss.MessageSinkConfig{
		NatsURL:   natsURL,
		ClusterID: q.Get("cluster-id"),
		ClientID:  q.Get("client-id"),
		Topic:     topic,
	})
}

var natsStreamingSinker = natss.NewMessageSink

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
