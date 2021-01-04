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
func NewConsumerClient(ctx context.Context, identifier string, bConfig *BrokerConfig, options *ConsumerClientOptions) (Consumer, error) {
	switch identifier {
	case Kafka:
		return NewKafkaConsumerClient(ctx, bConfig, options)
	case Pulsar:
		return NewPulsarConsumerClient(ctx, bConfig, options)
	}
	return nil, fmt.Errorf("unknown Broker identifier, %s", identifier)
}

// NewProducerClient returns an instance of a producer, kafka or pulsar
func NewProducerClient(ctx context.Context, identifier string, bConfig *BrokerConfig, options *ProducerClientOptions) (Producer, error) {
	switch identifier {
	case Kafka:
		return NewKafkaProducerClient(ctx, bConfig, options)
	case Pulsar:
		return NewPulsarProducerClient(ctx, bConfig, options)
	}
	return nil, fmt.Errorf("unknown Broker identifier, %s", identifier)
}

// NewAdminClient returns an instance of a admin, kafka or pulsar
func NewAdminClient(ctx context.Context, identifier string, bConfig *BrokerConfig, options *AdminClientOptions) (Admin, error) {
	switch identifier {
	case Kafka:
		return NewKafkaAdminClient(ctx, bConfig, options)
	case Pulsar:
		return NewPulsarAdminClient(ctx, bConfig, options)
	}
	return nil, fmt.Errorf("unknown Broker identifier, %s", identifier)
}
