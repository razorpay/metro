package config

import (
	"github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/tracing"
	"github.com/razorpay/metro/pkg/worker"
	"github.com/razorpay/metro/pkg/worker/queue"
)

type Config struct {
	App     App
	Sentry  *sentry.Config
	Auth    Auth
	Tracing tracing.Config
	Job     Job
	Queue   queue.Config
	Worker  worker.Config
	// Services is a map of service name to service config items
	Services map[string]Service
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
	Interfaces struct {
		Api NetworkInterfaces
	}
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

type Job struct {
	UserApprove string `mapstructure:"user_approve"`
}

type Producer struct {
	Variant string // kafka or pulsar
	Kafka   struct {
		ConnectionParams ConnectionParams
	}
	Pulsar struct {
		ConnectionParams ConnectionParams
	}
}

type ConnectionParams struct {
	Brokers []string
}
