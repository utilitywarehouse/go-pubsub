package sns_test

import (
	. "github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/utilitywarehouse/go-pubsub"
	"github.com/utilitywarehouse/go-pubsub/sns"
	"github.com/utilitywarehouse/go-pubsub/sns/mocks"
	"testing"
)

const topic = "test topic"

func TestSNSSink_PutMessage(t *testing.T) {
	mockCtrl := NewController(t)
	defer mockCtrl.Finish()
	mockAPI := mocks.NewMockSNSAPI(mockCtrl)

	sink := sns.NewTestSNSSink(mockAPI, topic)

	var payload pubsub.SimpleProducerMessage = []byte("simple payload")

	mockAPI.EXPECT().Publish(Any()).Times(1)
	err := sink.PutMessage(payload)
	if err != nil {
		t.Errorf("call to PutMessage() returned unexpected error %v", err)
	}

	status, err := sink.Status()
	if err != nil {
		t.Errorf("unexpected status error: %v", err)
	}

	if !status.Working {
		t.Error("status Working flag should be true because PutMessage succeed")
	}

	if len(status.Problems) != 0 {
		t.Error("status Problems should be empty, got %d: %v", len(status.Problems), status.Problems)
	}
}

func TestSNSSink_PutMessagePublishError(t *testing.T) {
	mockCtrl := NewController(t)
	defer mockCtrl.Finish()
	mockAPI := mocks.NewMockSNSAPI(mockCtrl)

	sink := sns.NewTestSNSSink(mockAPI, topic)
	var payload pubsub.SimpleProducerMessage = []byte("simple payload")

	testError := errors.New("test error")
	mockAPI.EXPECT().Publish(Any()).Times(1).Return(nil, testError)

	err := sink.PutMessage(payload)
	if err == nil {
		t.Errorf("PutMessage() failed to return expected error on Marshall().")
	}

	status, err := sink.Status()
	if err != nil {
		t.Errorf("unexpected status error: %v", err)
	}

	if status.Working {
		t.Error("status Working flag should be false because Publish returned an error")
	}

	if len(status.Problems) != 1 {
		t.Errorf("expected 1 status problem, got %d: %v", len(status.Problems), status.Problems)
	}

	if status.Problems[0] != testError.Error() {
		t.Errorf("status problem should be testError, got: %v", status.Problems[0])
	}
}

type ErrorPayload struct{}

func (e ErrorPayload) Marshal() ([]byte, error) {
	return nil, errors.New("an error")
}

func TestSNSSink_PutMessageMarshallError(t *testing.T) {
	mockCtrl := NewController(t)
	defer mockCtrl.Finish()
	mockAPI := mocks.NewMockSNSAPI(mockCtrl)

	sink := sns.NewTestSNSSink(mockAPI, topic)

	mockAPI.EXPECT().Publish(Any()).Times(0)
	err := sink.PutMessage(ErrorPayload{})
	if err == nil {
		t.Errorf("PutMessage() failed to return expected error on Marshall().")
	}

	status, err := sink.Status()
	if err != nil {
		t.Errorf("unexpected status error: %v", err)
	}

	if status.Working {
		t.Error("status Working flag should be false because Publish returned an error")
	}

	if len(status.Problems) != 1 {
		t.Errorf("expected 1 status problem, got %d: %v", len(status.Problems), status.Problems)
	}
}
