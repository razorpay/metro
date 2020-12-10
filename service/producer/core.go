package producer

import "github.com/razorpay/metro/pkg/messagebroker"

type core struct {
	Broker messagebroker.Broker
}

func newCore(broker messagebroker.Broker) (*core, error) {
	return &core{Broker: broker}, nil
}
