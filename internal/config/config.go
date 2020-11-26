package config

import (
	"github.com/razorpay/metro/pkg/tracing"
	"github.com/razorpay/metro/pkg/worker"
	"github.com/razorpay/metro/pkg/worker/queue"
)

type Config struct {
	App      App
	Sentry   Sentry
	Auth     Auth
	Tracing  tracing.Config
	Job      Job
	Queue    queue.Config
	Worker   worker.Config
	Producer Producer
}

// App contains application-specific config values
type App struct {
	Env             string
	ServiceName     string
	ShutdownTimeout int
	ShutdownDelay   int
	GitCommitHash   string
	Interfaces      struct {
		Api NetworkInterfaces
	}
}

type NetworkInterfaces struct {
	GrpcServerAddress     string
	HttpServerAddress     string
	InternalServerAddress string
}

type Sentry struct {
	DNS      string
	Enabled  bool
	LogLevel string
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
