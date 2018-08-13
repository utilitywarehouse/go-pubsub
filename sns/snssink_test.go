package sns_test

import (
	"github.com/aws/aws-sdk-go/aws"
	sns2 "github.com/aws/aws-sdk-go/service/sns"
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
}

func TestSNSSink_PutMessagePublishError(t *testing.T) {
	mockCtrl := NewController(t)
	defer mockCtrl.Finish()
	mockAPI := mocks.NewMockSNSAPI(mockCtrl)

	sink := sns.NewTestSNSSink(mockAPI, topic)
	var payload pubsub.SimpleProducerMessage = []byte("simple payload")

	mockAPI.EXPECT().Publish(Any()).Return(errors.New("test error")).Times(1)
	err := sink.PutMessage(payload)
	if err == nil {
		t.Error("PutMessage() failed to return expected error on Publish().")
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

	mockAPI.EXPECT().Publish(Any()).Times(1)
	err := sink.PutMessage(ErrorPayload{})
	if err == nil {
		t.Errorf("PutMessage() failed to return expected error on Marshall().")
	}
}

func TestSNSSink_Status(t *testing.T) {
	mockCtrl := NewController(t)
	defer mockCtrl.Finish()
	mockAPI := mocks.NewMockSNSAPI(mockCtrl)

	sink := sns.NewTestSNSSink(mockAPI, topic)
	topics := sns2.ListTopicsOutput{NextToken: aws.String("bleble")}
	topics.Topics = append(topics.Topics, &sns2.Topic{TopicArn: aws.String(topic)})

	mockAPI.EXPECT().ListTopics(Any()).Times(1).Return(&topics, nil)
	status, err := sink.Status()

	if err != nil {
		t.Errorf("Status() returned unexpected error %v", err)
	}

	if status.Working != true {
		t.Error("status.Working should be true")
	}

	if len(status.Problems) != 0 {
		t.Error("status.Problems should be empty")
	}
}

func TestSNSSink_StatusError(t *testing.T) {
	mockCtrl := NewController(t)
	defer mockCtrl.Finish()
	mockAPI := mocks.NewMockSNSAPI(mockCtrl)

	sink := sns.NewTestSNSSink(mockAPI, topic)
	mockAPI.EXPECT().ListTopics(Any()).Times(1).Return(nil, errors.New("test"))
	status, err := sink.Status()

	if status != nil {
		t.Error("expected returned status to be nil!")
	}

	if err == nil {
		t.Error("Expected Status() to return error")
	}
}

func TestSNSSink_StatusMissingTopic(t *testing.T) {
	mockCtrl := NewController(t)
	defer mockCtrl.Finish()
	mockAPI := mocks.NewMockSNSAPI(mockCtrl)

	sink := sns.NewTestSNSSink(mockAPI, "other")
	topics := sns2.ListTopicsOutput{NextToken: aws.String("")}
	topics.Topics = append(topics.Topics, &sns2.Topic{TopicArn: aws.String("some other topic")})

	mockAPI.EXPECT().ListTopics(Any()).Times(1).Return(&topics, nil)
	status, err := sink.Status()

	if err != nil {
		t.Errorf("unexpected error from Status(): %v", err)
	}

	if status.Working != false {
		t.Error("status.Working should be false")
	}

	if len(status.Problems) == 0 {
		t.Error("status.Problems should be populated")
	}
}
