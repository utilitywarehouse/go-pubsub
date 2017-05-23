package sqs

import (
	"encoding/json"
	"testing"

	pubsub "github.com/utilitywarehouse/go-pubsub"
)

type testMessage struct {
	Value      string            `json:"value"`
	Attributes map[string]string `json:"attributes"`
}

func (tm *testMessage) Marshal() ([]byte, error) {
	return json.Marshal(tm)
}

func (tm *testMessage) GetAttributes() Attributes {
	return tm.Attributes
}

func TestAttributesPropagate(t *testing.T) {
	var m pubsub.ProducerMessage = &testMessage{}
	_, ok := m.(Attributable)
	if !ok {
		t.Fatal("test message is not attributable")
	}
}
