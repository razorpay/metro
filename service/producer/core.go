package producer

import "github.com/razorpay/metro/pkg/messagebroker"

type Core struct {
	Broker messagebroker.Broker
}

func NewCore(broker messagebroker.Broker) (*Core, error) {
	return &Core{Broker: broker}, nil
}
