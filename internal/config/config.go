package config

import (
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/tracing"
)

type Config map[string]ServiceConfig

type ServiceConfig struct {
	App        App
	Sentry     *sentry.Config
	Tracing    tracing.Config
	Interfaces struct {
		Api NetworkInterfaces
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

type Service struct {
	Variant      string // kafka or pulsar
	BrokerConfig messagebroker.BrokerConfig
}

type NetworkInterfaces struct {
	GrpcServerAddress     string
	HttpServerAddress     string
	InternalServerAddress string
}

type Auth struct {
	Username string
	Password string
}
