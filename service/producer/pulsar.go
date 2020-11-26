package producer

import (
	"github.com/razorpay/metro/internal/config"
)

type PulsarProducer struct {
	// will hold the pulsar client impl

}

func newKPulsarProducer(config *config.ConnectionParams) IProducer {
	// init and return new instance
	return &PulsarProducer{}
}
