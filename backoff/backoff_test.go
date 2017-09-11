package backoff

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/mockqueue"

	"github.com/magiconair/properties/assert"
)

func TestBackOff(t *testing.T) {
	in := mockqueue.NewMockQueue()

	err := in.PutMessage(pubsub.SimpleProducerMessage([]byte("test")))
	if err != nil {
		t.Fatalf("error publishing message: [%+v]", err)
	}

	retryCount := 0
	ctx, cancel := context.WithCancel(context.Background())
	handler := func(m pubsub.ConsumerMessage) error {
		if retryCount == 3 {
			cancel()
			return nil
		}
		retryCount++
		return errors.New("error message")
	}
	boErrHanlder := pubsub.ConsumerErrorHandler(New(handler, 3))

	in.ConsumeMessages(ctx, handler, boErrHanlder)
	assert.Equal(t, retryCount, 3)
}
