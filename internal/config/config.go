package config

import (
	"github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/tracing"
	openapiserver "github.com/razorpay/metro/service/openapi-server"
	"github.com/razorpay/metro/service/producer"
	pullconsumer "github.com/razorpay/metro/service/pull-consumer"
	pushconsumer "github.com/razorpay/metro/service/push-consumer"
)

// Config is application config
type Config struct {
	App           App
	Tracing       tracing.Config
	Sentry        sentry.Config
	Producer      producer.Config
	PushConsumer  pushconsumer.Config
	PullConsumer  pullconsumer.Config
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
