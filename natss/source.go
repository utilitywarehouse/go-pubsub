package natss

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/utilitywarehouse/go-pubsub"
)

func generateID() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(random)
}

var _ pubsub.MessageSource = (*messageSource)(nil)

type MessageSourceConfig struct {
	NatsURL    string
	ClusterID  string
	Topic      string
	ConsumerID string

	//Offset - offset used for consuming messages
	Offset Offset

	//OffsetStartAtIndex - start delivering messages from this index
	OffsetStartAtIndex uint64

	//OffsetStartDuration - start delivering messages from this duration ago
	OffsetStartDuration time.Duration

	NonDurable bool
	AckWait    *time.Duration
}

//Offset - represents offset used for consuming msgs from the queue
type Offset int

type messageSource struct {
	natsURL             string
	clusterID           string
	consumerID          string
	topic               string
	offset              Offset
	offsetStartAtIndex  uint64
	offsetStartDuration time.Duration
	nonDurable          bool
	ackWait             *time.Duration
	conn                stan.Conn
}

const (
	//OffsetStartAt - to start at a given msg number"
	OffsetStartAt = Offset(iota)

	//OffsetDeliverLast - deliver from the last message
	OffsetDeliverLast

	//OffsetDeliverAll - deliver all the messages form the begining
	OffsetDeliverAll

	//OffsetStartDuration - deliver messages from a certain time
	OffsetStartDuration
)

func NewMessageSource(conf MessageSourceConfig) (pubsub.MessageSource, error) {
	return &messageSource{
		natsURL:             conf.NatsURL,
		clusterID:           conf.ClusterID,
		consumerID:          conf.ConsumerID,
		topic:               conf.Topic,
		offset:              conf.Offset,
		offsetStartAtIndex:  conf.OffsetStartAtIndex,
		offsetStartDuration: conf.OffsetStartDuration,
		nonDurable:          conf.NonDurable,
		ackWait:             conf.AckWait,
	}, nil
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler pubsub.ConsumerMessageHandler, onError pubsub.ConsumerErrorHandler) error {

	conn, err := stan.Connect(mq.clusterID, mq.consumerID+generateID(), stan.NatsURL(mq.natsURL))
	if err != nil {
		return err
	}
	mq.conn = conn
	defer conn.Close()

	natsConn := conn.NatsConn()

	anyError := make(chan error, 1)

	closedHandler := func(natsConnection *nats.Conn) {
		select {

		case anyError <- errors.New("underlying nats connection to " + mq.natsURL + " failed"):
		default:
		}
	}

	natsConn.SetDisconnectHandler(closedHandler)

	broken := false

	f := func(msg *stan.Msg) {

		if broken {
			return
		}

		m := pubsub.ConsumerMessage{msg.Data}
		err := handler(m)
		if err != nil {
			if err := onError(m, err); err != nil {
				broken = true
				select {
				case anyError <- err:
				default:
				}

			} else {
				msg.Ack()
			}
		} else {
			msg.Ack()
		}
	}

	//defalt to start from first index
	startOpt := stan.StartAt(pb.StartPosition_First)

	switch offset := mq.offset; offset {

	case OffsetStartAt:
		startOpt = stan.StartAtSequence(mq.offsetStartAtIndex)

	case OffsetDeliverLast:
		startOpt = stan.StartWithLastReceived()

	case OffsetDeliverAll:
		startOpt = stan.DeliverAllAvailable()

	case OffsetStartDuration:
		startOpt = stan.StartAtTimeDelta(mq.offsetStartDuration)
	}

	opts := []stan.SubscriptionOption{
		startOpt,
		stan.SetManualAckMode(),
	}
	if !mq.nonDurable {
		opts = append(opts, stan.DurableName(mq.consumerID))
	}
	if mq.ackWait != nil {
		opts = append(opts, stan.AckWait(*mq.ackWait))
	}

	subscription, err := conn.QueueSubscribe(mq.topic, mq.consumerID, f, opts...)
	if err != nil {
		return err
	}
	defer subscription.Close()

	select {
	case <-ctx.Done():
	case err = <-anyError:
	}

	return err
}

func (mq *messageSource) Status() (*pubsub.Status, error) {
	if mq.conn != nil {
		return natsStatus(*mq.conn.NatsConn()), nil
	}
	return nil, fmt.Errorf("nats connection not established yet")
}
