package natss

import (
	"sync"

	"github.com/pkg/errors"

	"github.com/nats-io/go-nats-streaming"
	"github.com/utilitywarehouse/go-pubsub"
)

var _ pubsub.MessageSink = (*messageSink)(nil)

type MessageSinkConfig struct {
	NatsURL   string
	ClusterID string
	Topic     string
	ClientID  string
}

type connectionState struct {
	sync.RWMutex
	err error
}

// ErrConnLost is the error created when the underlying nats streaming client
// has established that the connection to the nats streaming server has been lost
var ErrConnLost = errors.New("nats streaming connection lost")

type retryableNatsError struct {
	errString string
}

func (e *retryableNatsError) Error() string {
	return e.errString
}

func (e *retryableNatsError) Retryable() bool {
	return true
}

type messageSink struct {
	topic string
	sc    stan.Conn // nats streaming
	state connectionState
}

func NewMessageSink(config MessageSinkConfig) (pubsub.MessageSink, error) {
	sink := messageSink{topic: config.Topic}

	sc, err := stan.Connect(config.ClusterID, config.ClientID, stan.NatsURL(config.NatsURL), stan.SetConnectionLostHandler(func(_ stan.Conn, e error) {
		sink.state.Lock()
		sink.state.err = errors.Wrapf(ErrConnLost, "underlying error: %s", e.Error())
		sink.state.Unlock()
	}))
	if err != nil {
		return nil, errors.Wrapf(err, "connecting nats streaming client to cluster: %s at: %s", config.ClusterID, config.NatsURL)
	}
	sink.sc = sc
	return &sink, nil
}

func (mq *messageSink) PutMessage(m pubsub.ProducerMessage) error {
	mq.state.RLock()
	if mq.state.err != nil {
		mq.state.RUnlock()
		return mq.state.err
	}
	mq.state.RUnlock()
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	return mq.sc.Publish(mq.topic, data)
}

func (mq *messageSink) Close() error {
	return mq.sc.Close()
}

func (mq *messageSink) Status() (*pubsub.Status, error) {
	natsStatus, err := natsStatus(mq.sc.NatsConn())
	if err != nil {
		return nil, err
	}
	mq.state.RLock()
	if mq.state.err != nil {
		natsStatus.Working = false
		natsStatus.Problems = append(natsStatus.Problems, mq.state.err.Error())
	}
	mq.state.RUnlock()
	return natsStatus, nil
}
