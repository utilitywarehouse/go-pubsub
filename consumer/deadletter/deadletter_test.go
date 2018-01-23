package deadletter

import (
	"context"
	"testing"

	"encoding/json"
	"github.com/magiconair/properties/assert"
	"github.com/pkg/errors"
	"github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/mockqueue"
)

func TestDeadLetter(t *testing.T) {
	in := mockqueue.NewMockQueue()
	dl := mockqueue.NewMockQueue()

	msg := []byte("test")
	errMsg := "error message"
	consumerTopic := "consumerTopic"
	consumer := "consumer"

	err := in.PutMessage(pubsub.SimpleProducerMessage(msg))
	if err != nil {
		t.Fatalf("error publishing message: [%+v]", err)
	}

	inCtx, inCancel := context.WithCancel(context.Background())
	handler := func(m pubsub.ConsumerMessage) error {
		inCancel()
		return errors.New(errMsg)
	}
	dlErrHanlder := pubsub.ConsumerErrorHandler(New(dl, consumerTopic, consumer))

	in.ConsumeMessages(inCtx, handler, dlErrHanlder)

	var dlMsg []byte
	dlCtx, dlCancel := context.WithCancel(context.Background())
	dlHandler := func(m pubsub.ConsumerMessage) error {
		dlMsg = m.Data
		dlCancel()
		return nil
	}
	errHandler := func(m pubsub.ConsumerMessage, e error) error {
		t.Error("unexpected error")
		return nil
	}

	dl.ConsumeMessages(dlCtx, dlHandler, errHandler)

	fsm := FailedConsumerMessage{}
	err = json.Unmarshal(dlMsg, &fsm)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, fsm.Message, msg)
	assert.Equal(t, fsm.Err, errMsg)
	assert.Equal(t, fsm.MessageTopic, consumerTopic)
	assert.Equal(t, fsm.Consumer, consumer)
}
