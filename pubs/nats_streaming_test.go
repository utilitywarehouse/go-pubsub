package pubs

import (
	"github.com/stretchr/testify/assert"
	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/natss"
	"testing"
)

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
