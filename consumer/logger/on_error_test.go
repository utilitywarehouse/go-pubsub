package logger_test

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	pubsub "github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/consumer/logger"
	mock "github.com/utilitywarehouse/go-pubsub/consumer/logger/mock"
)

func TestOnErrorLog(t *testing.T) {
	errConnection := errors.New("connection timeout")

	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	mockLogger := mock.NewMockLogger(ctrl)
	mockLogger.EXPECT().Error(errConnection)

	onError := logger.NewErrorHandler(mockLogger, nil)
	err := onError(pubsub.ConsumerMessage{}, errConnection)
	assert.Nil(t, err)
}

func TestOnErrorFallback(t *testing.T) {
	errConnection := errors.New("connection timeout")

	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	mockLogger := mock.NewMockLogger(ctrl)
	mockLogger.EXPECT().Error(gomock.Any())

	onErrorFallback := func(msg pubsub.ConsumerMessage, err error) error {
		return err
	}
	onError := logger.NewErrorHandler(mockLogger, onErrorFallback)

	actualErr := onError(pubsub.ConsumerMessage{}, errConnection)
	assert.Equal(t, actualErr, errConnection)
}
