package instrumented_test

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/instrumented"
	"github.com/utilitywarehouse/go-pubsub/mocks"
	"sync"
	"testing"
)

//go:generate mockery -dir ../ -name MessageSink --output ../mocks -case underscore
//go:generate mockery -dir ../ -name ProducerMessage --output ../mocks -case underscore

func TestNewReconnectorSink(t *testing.T) {
	sink, err := instrumented.NewReconnectorSink(
		func() (pubsub.MessageSink, error) {
			return &mocks.MessageSink{}, nil
		},
		&instrumented.ReconnectionOptions{},
	)

	if err != nil {
		t.Fail()
	}

	assert.Nil(t, err)
	assert.NotNil(t, sink)
}

func TestWillReconnectAsynchronouslyWhenPutMessageDetectADisconnectionError(t *testing.T) {
	sinkMock := &mocks.MessageSink{}

	wg := sync.WaitGroup{}
	wg.Add(1)

	sink, err := instrumented.NewReconnectorSink(func() (pubsub.MessageSink, error) {
		return sinkMock, nil
	}, &instrumented.ReconnectionOptions{
		OnReconnectSuccess: func(sink pubsub.MessageSink, err error) {
			wg.Done()
		},
	})

	if err != nil {
		t.Fail()
	}

	called := 0
	sinkMock.
		On("PutMessage", &mocks.ProducerMessage{}).
		Return(func (m pubsub.ProducerMessage) (error) {
			if called == 0 {
				called++
				return errors.New("broker can't be reached")
			}
			return nil
		})

	sinkMock.
		On("Status").
		Return(&pubsub.Status{
			Working: false,
		}, nil)

	sinkMock.On("Close").Return(nil)

	err = sink.PutMessage(&mocks.ProducerMessage{})

	// Will return error right away ( re-connection happens under the hood )
	assert.NotNil(t, err)

	wg.Wait()

	// Re-Calling put message should now succeed
	err = sink.PutMessage(&mocks.ProducerMessage{})

	assert.Nil(t, err)
}

func TestWillReconnectSynchronouslyWhenPutMessageDetectADisconnectionError(t *testing.T) {
	sinkMock := &mocks.MessageSink{}

	sink, err := instrumented.NewReconnectorSink(func() (pubsub.MessageSink, error) {
		return sinkMock, nil
	}, &instrumented.ReconnectionOptions{
		ReconnectSynchronously: true,
	})

	if err != nil {
		t.Fail()
	}

	putMessageCalled := 0
	sinkMock.
		On("PutMessage", &mocks.ProducerMessage{}).
		Return(func (m pubsub.ProducerMessage) (error) {
			if putMessageCalled == 0 {
				putMessageCalled++
				return errors.New("broker can't be reached")
			}
			return nil
		})

	statusCalled := 0
	sinkMock.
		On("Status").
		Return(func () (*pubsub.Status) {
			if statusCalled == 0 {
				statusCalled++
				return &pubsub.Status{
					Working: false,
				}
			}
			return &pubsub.Status{
				Working: true,
			}
		}, nil)

	sinkMock.On("Close").Return(nil)

	err = sink.PutMessage(&mocks.ProducerMessage{})

	// reconnection happened synchronously
	assert.Nil(t, err)
}