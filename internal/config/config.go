package config

import (
	"github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/razorpay/metro/pkg/tracing"
	openapiserver "github.com/razorpay/metro/service/openapi-server"
	"github.com/razorpay/metro/service/web"
	worker "github.com/razorpay/metro/service/worker"
)

// Config is application config
type Config struct {
	App           App
	Tracing       tracing.Config
	Sentry        sentry.Config
	Web           web.Config
	Worker        worker.Config
	Registry      registry.Config
	OpenAPIServer openapiserver.Config
}

// App contains application-specific config values
type App struct {
	Env             string
	ServiceName     string
	ShutdownTimeout int
	ShutdownDelay   int
	GitCommitHash   string
}
