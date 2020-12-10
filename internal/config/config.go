package config

import (
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/tracing"
)

// Config for metro
type Config struct {
	App     App
	Sentry  *sentry.Config
	Tracing tracing.Config
	// Components is a map of service name to service config items
	Components map[string]Component
}

// App contains application-specific config values
type App struct {
	Env             string
	ServiceName     string
	ShutdownTimeout int
	ShutdownDelay   int
	GitCommitHash   string
}

// Component specific configuration
type Component struct {
	Interfaces struct {
		API NetworkInterfaces
	}
	Variant      string // kafka or pulsar
	BrokerConfig messagebroker.BrokerConfig
}

// NetworkInterfaces contains all exposed interfaces
type NetworkInterfaces struct {
	GrpcServerAddress     string
	HTTPServerAddress     string
	InternalServerAddress string
}
