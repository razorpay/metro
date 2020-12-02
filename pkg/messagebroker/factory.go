package messagebroker

import (
	"github.com/razorpay/metro/pkg/messagebroker/kafka"
)

func GetConsumer(identifier string, topic string, config interface{}) MessageConsumer {
	switch identifier {
	case "kafka":
		return kafka.NewConsumer(topic, config)
	}

	return kafka.NewConsumer(topic, config)
}
