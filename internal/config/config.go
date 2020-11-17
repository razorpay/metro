package config

import (
	"github.com/razorpay/metro/pkg/spine/db"
	"github.com/razorpay/metro/pkg/tracing"
	"github.com/razorpay/metro/pkg/worker"
	"github.com/razorpay/metro/pkg/worker/queue"
)

type Config struct {
	App     App
	Db      db.Config
	Sentry  Sentry
	Auth    Auth
	Tracing tracing.Config
	Job     Job
	Queue   queue.Config
	Worker  worker.Config
}

// App contains application-specific config values
type App struct {
	Env             string
	ServiceName     string
	Hostname        string
	Port            string
	ShutdownTimeout int
	ShutdownDelay   int
	GitCommitHash   string
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
