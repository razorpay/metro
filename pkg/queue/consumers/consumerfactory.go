package consumer

import (
	"github.com/razorpay/metro/pkg/queue/consumers/kafka"
)

type ConsumerFactory struct {
}

func (factory *ConsumerFactory) GetConsumer(identifier string, config interface{}) QueueConsumer {
	switch identifier {
	case "kafka":
		return kafka.NewConsumer(config)
	}

	return kafka.NewConsumer(config)
}
