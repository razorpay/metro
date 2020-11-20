package producer

import (
	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/config"
)

type KafkaProducer struct {
	// will hold the kafka client impl
}

func NewKakfaProducer(config *config.ConnectionParams) IProducer {
	// init and return new instance
	return &KafkaProducer{}
}

func (k *KafkaProducer) PublishMessage(topic string, message []byte) (string, error) {
	return uuid.New().String(), nil
}
