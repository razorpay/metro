package worker

import (
	"github.com/razorpay/metro/pkg/messagebroker"
)

// Config for pushconsumer
type Config struct {
	Broker     Broker
	Interfaces struct {
		API NetworkInterfaces
	}
	HTTPClientConfig HTTPClientConfig
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

// HTTPClientConfig contains config the init a new http client
type HTTPClientConfig struct {
	ConnectTimeoutMS        int
	ConnKeepAliveMS         int
	ExpectContinueTimeoutMS int
	IdleConnTimeoutMS       int
	MaxAllIdleConns         int
	MaxHostIdleConns        int
	ResponseHeaderTimeoutMS int
	TLSHandshakeTimeoutMS   int
}
