package proximo

import (
	"errors"

	"github.com/utilitywarehouse/go-pubsub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// ErrNotConnected is returned if a status is requested before the connection has been initialized
var ErrNotConnected = errors.New("proximo not connected")

func sinkStatus(cc *grpc.ClientConn) (*pubsub.Status, error) {
	if cc == nil {
		return nil, ErrNotConnected
	}

	var (
		working  bool
		problems []string
	)
	switch cc.GetState() {
	case connectivity.Ready,
		connectivity.Idle:
		working = true
	case connectivity.Shutdown,
		connectivity.TransientFailure:
		working = false
		problems = append(problems, cc.GetState().String())
	case connectivity.Connecting:
		working = true
		problems = append(problems, cc.GetState().String())
	default:
		working = false
		problems = append(problems, "unknown connectivity state")
	}

	return &pubsub.Status{
		Problems: problems,
		Working:  working,
	}, nil
}
