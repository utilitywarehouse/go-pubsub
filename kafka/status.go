package kafka

import (
	"net"
	"time"

	"github.com/utilitywarehouse/go-pubsub"
)

func status(brokers []string) (*pubsub.Status, error) {
	errs := make(chan error)

	for _, broker := range brokers {
		go func(broker string) {
			conn, err := net.DialTimeout("tcp", broker, 10*time.Second)
			if err != nil {
				errs <- fmt.Errorf("Failed to connect to broker %s: %v", err)
				return
			}
			if err = conn.Close(); err != nil {
				errs <- fmt.Errorf("Failed to close connection to broker %s: %v", err)
				return
			}
			errs <- nil
		}(broker)
	}

	s := &pubsub.Status{}
	for range brokers {
		err := <-errs
		if err != nil {
			s.Problems = append(s.Problems, err.Error())
		} else {
			s.Working = true
		}
	}
	return s, nil
}
