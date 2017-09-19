package pubs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/kafka"
	"github.com/utilitywarehouse/go-pubsub/natss"
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

	tests := []struct {
		name        string
		input       string
		expected    natss.MessageSinkConfig
		expectedErr bool
	}{
		{
			name:  "simple",
			input: "nats-streaming://localhost/t1",
			expected: natss.MessageSinkConfig{
				NatsURL: "nats://localhost",
				Topic:   "t1",
			},
			expectedErr: false,
		},
		{
			name:  "simple-trailing-slash",
			input: "nats-streaming://localhost/t1/",
			expected: natss.MessageSinkConfig{
				NatsURL: "nats://localhost",
				Topic:   "t1",
			},
			expectedErr: false,
		},
		{
			name:  "with-port",
			input: "nats-streaming://localhost:123/t1",
			expected: natss.MessageSinkConfig{
				NatsURL: "nats://localhost:123",
				Topic:   "t1",
			},
			expectedErr: false,
		},
		{
			name:  "everything",
			input: "nats-streaming://localhost:123/t1?cluster-id=cid-1&client-id=client-1",
			expected: natss.MessageSinkConfig{
				NatsURL:   "nats://localhost:123",
				ClusterID: "cid-1",
				ClientID:  "client-1",
				Topic:     "t1",
			},
			expectedErr: false,
		},
		{
			name:        "extra-path-elements",
			input:       "nats-streaming://localhost:123/aa/bb",
			expected:    natss.MessageSinkConfig{},
			expectedErr: true,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			var c natss.MessageSinkConfig
			natsStreamingSinker = func(conf natss.MessageSinkConfig) (pubsub.MessageSink, error) {
				c = conf
				return nil, nil
			}
			_, err := NewSink(tst.input)

			if tst.expectedErr == (err == nil) {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, c)
		})
	}

}
