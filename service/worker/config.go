package worker

import (
	"github.com/razorpay/metro/pkg/httpclient"
	"github.com/razorpay/metro/pkg/messagebroker"
)

// Config for pushconsumer
type Config struct {
	Fetch      Fetch
	Broker     Broker
	Interfaces struct {
		API NetworkInterfaces
	}
	HTTPClientConfig httpclient.Config
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

// Fetch contains config required for migrating to ingestion topics
type Fetch struct {
	FetchTopic             string
	ReadOffsetFromRegistry bool
}
