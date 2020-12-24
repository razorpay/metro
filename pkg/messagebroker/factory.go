package messagebroker

import (
	"context"
	"fmt"
)

const (
	// Kafka identifier
	Kafka = "kafka"
	// Pulsar identifier
	Pulsar = "pulsar"
)

// NewConsumer returns an instance of a consumer, kafka or pulsar
func NewConsumer(ctx context.Context, identifier string, bConfig *BrokerConfig) (Consumer, error) {
	switch identifier {
	case Kafka:
		return NewKafkaConsumer(ctx, bConfig)
	case Pulsar:
		return NewPulsarConsumer(ctx, bConfig)
	}
	return nil, fmt.Errorf("unknown Broker identifier, %s", identifier)
}

// NewProducer returns an instance of a producer, kafka or pulsar
func NewProducer(ctx context.Context, identifier string, bConfig *BrokerConfig) (Producer, error) {
	switch identifier {
	case Kafka:
		return NewKafkaProducer(ctx, bConfig)
	case Pulsar:
		return NewPulsarProducer(ctx, bConfig)
	}
	return nil, fmt.Errorf("unknown Broker identifier, %s", identifier)
}

// NewAdmin returns an instance of a admin, kafka or pulsar
func NewAdmin(ctx context.Context, identifier string, bConfig *BrokerConfig) (Admin, error) {
	switch identifier {
	case Kafka:
		return NewKafkaAdmin(ctx, bConfig)
	case Pulsar:
		return NewPulsarAdmin(ctx, bConfig)
	}
	return nil, fmt.Errorf("unknown Broker identifier, %s", identifier)
}
