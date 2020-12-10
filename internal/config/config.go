package config

import (
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/tracing"
)

// Config is application config
type Config map[string]ComponentConfig

// ComponentConfig for Metro components
type ComponentConfig struct {
	App        App
	Sentry     *sentry.Config
	Tracing    tracing.Config
	Broker     Broker
	Interfaces struct {
		API NetworkInterfaces
	}
}

// App contains application-specific config values
type App struct {
	Env             string
	ServiceName     string
	ShutdownTimeout int
	ShutdownDelay   int
	GitCommitHash   string
}

// Broker Config (Kafka/Pulsar)
type Broker struct {
	Variant      string // kafka or pulsar
	BrokerConfig messagebroker.BrokerConfig
}

// NetworkInterfaces contains all exposed interfaces
type NetworkInterfaces struct {
	GrpcServerAddress     string
	HTTPServerAddress     string
	InternalServerAddress string
}
