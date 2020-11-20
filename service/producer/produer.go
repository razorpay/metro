package producer

import (
	"github.com/razorpay/metro/internal/config"
)

type IProducer interface {
	PublishMessage(topic string, message []byte) (string, error)
}

const (
	VariantKafka = iota
	VariantPulsar
)

func NewProducer(config config.Producer) IProducer {
	switch config.Variant {
	case string(VariantKafka):
		return NewKakfaProducer(&config.Kafka)
	case string(VariantPulsar):
		return NewKPulsarProducer(&config.Pulsar)

	default:
		//panic("invalid producer variant configured")
		return NewKPulsarProducer(&config.Pulsar)
	}
}
