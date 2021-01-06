package pullconsumer

import (
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
)

// Config for pullconsumer
type Config struct {
	Broker     Broker
	Registry   registry.Config
	Interfaces struct {
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
	GrpcServerAddress string
	HTTPServerAddress string
}
