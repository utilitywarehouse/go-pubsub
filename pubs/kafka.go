package pubs

import (
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/kafka"
)

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
