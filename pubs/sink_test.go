package pubs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/kafka"
)

func TestKafkaSink(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    kafka.MessageSinkConfig
		expectedErr error
	}{
		{
			name:  "simple",
			input: "kafka://localhost",
			expected: kafka.MessageSinkConfig{
				Brokers: []string{"localhost"},
			},
			expectedErr: nil,
		},
		{
			name:  "standard",
			input: "kafka://localhost:123/t1",
			expected: kafka.MessageSinkConfig{
				Brokers: []string{"localhost:123"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "with-port",
			input: "kafka://localhost:123/t1",
			expected: kafka.MessageSinkConfig{
				Brokers: []string{"localhost:123"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "everything",
			input: "kafka://localhost:123/t1/?broker=localhost:234&broker=localhost:345",
			expected: kafka.MessageSinkConfig{
				Brokers: []string{"localhost:123", "localhost:234", "localhost:345"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {

			var conf kafka.MessageSinkConfig
			kafkaSinker = func(c kafka.MessageSinkConfig) (pubsub.MessageSink, error) {
				conf = c
				return nil, nil
			}
			_, err := NewSink(tst.input)

			if tst.expectedErr != err {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, conf)
		})
	}

}

func TestNatsStreamingSink(t *testing.T) {
	assert := assert.New(t)

	type natsConf struct{ natsURL, clusterID, consumerID, topic string }

	tests := []struct {
		name        string
		input       string
		expected    natsConf
		expectedErr bool
	}{
		{
			name:  "simple",
			input: "nats-streaming://localhost/t1",
			expected: natsConf{
				natsURL: "nats://localhost",
				topic:   "t1",
			},
			expectedErr: false,
		},
		{
			name:  "simple-trailing-slash",
			input: "nats-streaming://localhost/t1/",
			expected: natsConf{
				natsURL: "nats://localhost",
				topic:   "t1",
			},
			expectedErr: false,
		},
		{
			name:  "with-port",
			input: "nats-streaming://localhost:123/t1",
			expected: natsConf{
				natsURL: "nats://localhost:123",
				topic:   "t1",
			},
			expectedErr: false,
		},
		{
			name:  "everything",
			input: "nats-streaming://localhost:123/t1?cluster-id=cid-1&client-id=cons-1",
			expected: natsConf{
				natsURL:    "nats://localhost:123",
				clusterID:  "cid-1",
				consumerID: "cons-1",
				topic:      "t1",
			},
			expectedErr: false,
		},
		{
			name:        "extra-path-elements",
			input:       "nats-streaming://localhost:123/aa/bb",
			expected:    natsConf{},
			expectedErr: true,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {

			var conf natsConf
			natsStreamingSinker = func(natsURL, clusterID, consumerID, topic string) (pubsub.MessageSink, error) {
				conf.natsURL = natsURL
				conf.clusterID = clusterID
				conf.consumerID = consumerID
				conf.topic = topic
				return nil, nil
			}
			_, err := NewSink(tst.input)

			if tst.expectedErr == (err == nil) {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, conf)
		})
	}

}
