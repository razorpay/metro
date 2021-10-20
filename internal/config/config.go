package config

import (
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/pkg/cache"
	"github.com/razorpay/metro/pkg/encryption"
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
	Cache         cache.Config
	OpenAPIServer openapiserver.Config
	Admin         credentials.Model
	Encryption    encryption.Config
}

// App contains application-specific config values
type App struct {
	Env             string
	ServiceName     string
	ShutdownTimeout int
	ShutdownDelay   int
	GitCommitHash   string
}
