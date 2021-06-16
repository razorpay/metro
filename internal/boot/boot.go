package boot

import (
	"context"

	"github.com/razorpay/metro/internal/app"
	"github.com/razorpay/metro/internal/config"
	logpkg "github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/monitoring/sentry"
	sentrypkg "github.com/razorpay/metro/pkg/monitoring/sentry"
	tracingpkg "github.com/razorpay/metro/pkg/tracing"
)

// InitMonitoring is used to setup logger, tracing and sentry for monitoring
func InitMonitoring(env string, config config.App, sentry sentry.Config, tracing tracingpkg.Config) error {
	// Initializes Sentry monitoring client.
	s, err := sentrypkg.InitSentry(&sentry, env)
	if err != nil {
		return err
	}

	// Initializes logging driver.
	servicekv := map[string]interface{}{
		"appEnv":        app.GetEnv(),
		"serviceName":   config.ServiceName,
		"gitCommitHash": config.GitCommitHash,
	}
	logger, err := logpkg.NewLogger(env, servicekv, s)
	if err != nil {
		return err
	}
	err = tracingpkg.Init(tracing, logger.Desugar())
	if err != nil {
		return err
	}

	return nil
}

// Close is used to stop any init component
func Close() error {
	// Close the tracer
	err := tracingpkg.Close()

	return err
}

// NewContext adds core key-value e.g. service name, git hash etc to
// existing context or to a new background context and returns.
func NewContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return ctx
}
