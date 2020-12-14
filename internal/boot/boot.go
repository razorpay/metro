package boot

import (
	"context"
	"io"
	"os"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/config"
	logpkg "github.com/razorpay/metro/pkg/logger"
	sentrypkg "github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/tracing"
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
func InitMonitoring(env string, config *config.ComponentConfig) error {
	// Initializes Sentry monitoring client.
	sentry, err := sentrypkg.InitSentry(config.Sentry, env)
	if err != nil {
		return err
	}
	// Initializes logging driver.
	servicekv := map[string]interface{}{
		"appEnv":        config.App.Env,
		"serviceName":   config.App.ServiceName,
		"gitCommitHash": config.App.GitCommitHash,
	}
	logger, err := logpkg.NewLogger(env, servicekv, sentry)
	if err != nil {
		return err
	}
	Tracer, Closer, err = tracing.Init(config.Tracing, logger.Desugar())
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
