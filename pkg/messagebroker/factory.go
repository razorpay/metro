package messagebroker

import (
	"github.com/razorpay/metro/pkg/messagebroker/kafka"
)

func GetConsumer(identifier string, config interface{}) QueueConsumer {
	switch identifier {
	case "kafka":
		return kafka.NewConsumer(config)
	}

	return kafka.NewConsumer(config)
}
