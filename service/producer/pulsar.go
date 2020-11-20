package producer

import (
	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/config"
)

type PulsarProducer struct {
	// will hold the pulsar client impl

}

func NewKPulsarProducer(config *config.ConnectionParams) IProducer {
	// init and return new instance
	return &PulsarProducer{}
}

func (k *PulsarProducer) PublishMessage(topic string, message []byte) (string, error) {
	return uuid.New().String(), nil
}
