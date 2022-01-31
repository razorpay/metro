package consumeplane

import (
	"github.com/razorpay/metro/pkg/messagebroker"
)

// Config for consumer
type Config struct {
	Broker       Broker
	ReplicaCount int
	OrdinalID    int
	Interfaces   struct {
		API NetworkInterfaces
	}
}

// Broker Config (Kafka/Pulsar)
type Broker struct {
	Variant      string // kafka or pulsar
	BrokerConfig messagebroker.BrokerConfig
}

// NetworkInterfaces contains all exposed interfaces
type NetworkInterfaces struct {
	GrpcServerAddress         string
	HTTPServerAddress         string
	InternalHTTPServerAddress string
}
