package boot

import (
	"context"
	"io"
	"os"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/config"
	logpkg "github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/monitoring/sentry"
	sentrypkg "github.com/razorpay/metro/pkg/monitoring/sentry"
	tracingpkg "github.com/razorpay/metro/pkg/tracing"
)

var (
	// Tracer is used for creating spans for distributed tracing
	Tracer opentracing.Tracer
	// Closer holds an instance to the RequestTracing object's Closer.
	Closer io.Closer
)

// GetEnv returns the current environment, prod, dev etc
func GetEnv() string {
	// Fetch env for bootstrapping
	environment := os.Getenv("APP_ENV")
	if environment == "" {
		environment = "dev"
	}

	return environment
}

// InitMonitoring is used to setup logger, tracing and sentry for monitoring
func InitMonitoring(env string, app config.App, sentry sentry.Config, tracing tracingpkg.Config) error {
	// Initializes Sentry monitoring client.
	s, err := sentrypkg.InitSentry(&sentry, env)
	if err != nil {
		return err
	}

	// Initializes logging driver.
	servicekv := map[string]interface{}{
		"appEnv":        app.Env,
		"serviceName":   app.ServiceName,
		"gitCommitHash": app.GitCommitHash,
	}
	logger, err := logpkg.NewLogger(env, servicekv, s)
	if err != nil {
		return err
	}
	Tracer, Closer, err = tracingpkg.Init(tracing, logger.Desugar())
	if err != nil {
		return err
	}

	return nil
}

// NewContext adds core key-value e.g. service name, git hash etc to
// existing context or to a new background context and returns.
func NewContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return ctx
}
