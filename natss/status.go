package natss

import (
	"fmt"

	nats "github.com/nats-io/go-nats"
	pubsub "github.com/utilitywarehouse/go-pubsub"
)

func natsStatus(nc nats.Conn) *pubsub.Status {
	working := nc.IsConnected()
	var problems []string
	if !working {
		notConnected := "nats not connected"
		if lastErr := nc.LastError(); lastErr != nil {
			notConnected = fmt.Sprintf("%s - last error: %s", notConnected, lastErr.Error())
		}
		problems = append(problems, notConnected)
	}
	return &pubsub.Status{
		Problems: problems,
		Working:  working,
	}
}
