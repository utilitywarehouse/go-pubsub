package pubs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/kafka"
	"github.com/utilitywarehouse/go-pubsub/natss"
)

func TestKafka(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    kafka.MessageSourceConfig
		expectedErr error
	}{
		{
			name:  "simple",
			input: "kafka://localhost",
			expected: kafka.MessageSourceConfig{
				Brokers: []string{"localhost"},
			},
			expectedErr: nil,
		},
		{
			name:  "standard",
			input: "kafka://localhost:123/t1",
			expected: kafka.MessageSourceConfig{
				Brokers: []string{"localhost:123"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "with-port",
			input: "kafka://localhost:123/t1",
			expected: kafka.MessageSourceConfig{
				Brokers: []string{"localhost:123"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "everything",
			input: "kafka://localhost:123/t1/?offset=latest&consumer-group=g1&metadata-refresh=2s&broker=localhost:234&broker=localhost:345",
			expected: kafka.MessageSourceConfig{
				Brokers:                  []string{"localhost:123", "localhost:234", "localhost:345"},
				ConsumerGroup:            "g1",
				MetadataRefreshFrequency: 2 * time.Second,
				Offset: kafka.OffsetLatest,
				Topic:  "t1",
			},
			expectedErr: nil,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {

			var conf kafka.MessageSourceConfig
			kafkaSourcer = func(c kafka.MessageSourceConfig) pubsub.MessageSource {
				conf = c
				return nil
			}
			_, err := NewSource(tst.input)

			if tst.expectedErr != err {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, conf)
		})
	}

}

func TestNatsStreaming(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    natss.MessageSourceConfig
		expectedErr bool
	}{
		{
			name:  "simple",
			input: "nats-streaming://localhost/t1",
			expected: natss.MessageSourceConfig{
				NatsURL: "nats://localhost",
				Topic:   "t1",
			},
			expectedErr: false,
		},
		{
			name:  "simple-trailing-slash",
			input: "nats-streaming://localhost/t1/",
			expected: natss.MessageSourceConfig{
				NatsURL: "nats://localhost",
				Topic:   "t1",
			},
			expectedErr: false,
		},
		{
			name:  "with-port",
			input: "nats-streaming://localhost:123/t1",
			expected: natss.MessageSourceConfig{
				NatsURL: "nats://localhost:123",
				Topic:   "t1",
			},
			expectedErr: false,
		},
		{
			name:  "everything",
			input: "nats-streaming://localhost:123/t1?cluster-id=cid-1&consumer-id=cons-1",
			expected: natss.MessageSourceConfig{
				NatsURL:    "nats://localhost:123",
				ClusterID:  "cid-1",
				ConsumerID: "cons-1",
				Topic:      "t1",
			},
			expectedErr: false,
		},
		{
			name:        "extra-path-elements",
			input:       "nats-streaming://localhost:123/aa/bb",
			expected:    natss.MessageSourceConfig{},
			expectedErr: true,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			var c natss.MessageSourceConfig
			natsStreamingSourcer = func(conf natss.MessageSourceConfig) (pubsub.MessageSource, error) {
				c = conf
				return nil, nil
			}
			_, err := NewSource(tst.input)

			if tst.expectedErr == (err == nil) {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, c)
		})
	}

}
