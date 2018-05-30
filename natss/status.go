package natss

import (
	"errors"
	"fmt"

	nats "github.com/nats-io/go-nats"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

var ErrNotConnected = errors.New("nats not connected")

func natsStatus(nc *nats.Conn) (*pubsub.Status, error) {
	if nc == nil {
		return nil, ErrNotConnected
	}
	working := nc.IsConnected()
	var problems []string
	if !working {
		notConnected := ErrNotConnected.Error()
		if lastErr := nc.LastError(); lastErr != nil {
			notConnected = fmt.Sprintf("%s - last error: %s", notConnected, lastErr.Error())
		}
		problems = append(problems, notConnected)
	}
	return &pubsub.Status{
		Problems: problems,
		Working:  working,
	}, nil
}
