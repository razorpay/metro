package producer

import (
	"github.com/razorpay/metro/internal/config"
)

type Message struct {
	topic   string
	message []byte
}

type IProducer interface {
	PublishMessage(message *Message) (string, error)
}

const (
	VariantKafka = iota
	VariantPulsar
)

func NewProducer(config config.Producer) IProducer {
	switch config.Variant {
	case string(VariantKafka):
		return newKakfaProducer(&config.Kafka.ConnectionParams)
	case string(VariantPulsar):
		return newKPulsarProducer(&config.Pulsar.ConnectionParams)

	default:
		panic("invalid producer variant configured")
	}
}
