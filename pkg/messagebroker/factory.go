package messagebroker

import (
	"fmt"
)

const (
	Kafka  = "kafka"
	Pulsar = "pulsar"
)

func NewBroker(identifier string, bConfig *BrokerConfig) (Broker, error) {
	fmt.Printf("id : %s", identifier)
	switch identifier {
	case Kafka:
		return NewKafkaBroker(nil, bConfig)
	case Pulsar:
		return NewPulsarBroker(nil, bConfig)
	}

	return nil, fmt.Errorf("Unknown Broker identifier, %s", identifier)
}
