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

// NewConsumerClient returns an instance of a consumer, kafka or pulsar
func NewConsumerClient(ctx context.Context, variant string, id string, bConfig *BrokerConfig, options *ConsumerClientOptions) (Consumer, error) {
	switch variant {
	case Kafka:
		return newKafkaConsumerClient(ctx, bConfig, id, options)
	case Pulsar:
		return newPulsarConsumerClient(ctx, bConfig, id, options)
	}
	return nil, fmt.Errorf("unknown Broker variant, %s", variant)
}

// NewProducerClient returns an instance of a producer, kafka or pulsar
func NewProducerClient(ctx context.Context, identifier string, bConfig *BrokerConfig, options *ProducerClientOptions) (Producer, error) {
	switch identifier {
	case Kafka:
		return newKafkaProducerClient(ctx, bConfig, options)
	case Pulsar:
		return newPulsarProducerClient(ctx, bConfig, options)
	}
	return nil, fmt.Errorf("unknown Broker identifier, %s", identifier)
}

// NewAdminClient returns an instance of a admin, kafka or pulsar
func NewAdminClient(ctx context.Context, identifier string, bConfig *BrokerConfig, options *AdminClientOptions) (Admin, error) {
	switch identifier {
	case Kafka:
		return newKafkaAdminClient(ctx, bConfig, options)
	case Pulsar:
		return newPulsarAdminClient(ctx, bConfig, options)
	}
	return nil, fmt.Errorf("unknown Broker identifier, %s", identifier)
}
