package boot

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/config"
	config_reader "github.com/razorpay/metro/pkg/config"
	logpkg "github.com/razorpay/metro/pkg/logger"
	sentrypkg "github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/tracing"
)

var (
	// Config contains application configuration values.
	ComponentConfig config.ComponentConfig

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

// initialize all core dependencies for the application
func initialize(ctx context.Context, env string, component string) error {
	// read the component config for env
	var appConfig config.Config
	err := config_reader.NewDefaultConfig().Load(env, &appConfig)
	if err != nil {
		log.Fatal(err)
	}

	componentConfig, ok := appConfig[component]

	if !ok {
		log.Fatal(fmt.Errorf("`%v` component missing config", component))
	}

	ComponentConfig = componentConfig

	// Initializes Sentry monitoring client.
	sentry, err := sentrypkg.InitSentry(ComponentConfig.Sentry, env)
	if err != nil {
		return err
	}
	// Initializes logging driver.
	servicekv := map[string]interface{}{
		"appEnv":        ComponentConfig.App.Env,
		"serviceName":   ComponentConfig.App.ServiceName,
		"gitCommitHash": ComponentConfig.App.GitCommitHash,
	}
	logger, err := logpkg.NewLogger(env, servicekv, sentry)
	if err != nil {
		return err
	}
	Tracer, Closer, err = tracing.Init(ComponentConfig.Tracing, logger.Desugar())
	if err != nil {
		return err
	}

	return nil
}

func InitMetro(ctx context.Context, env string, service string) error {
	err := initialize(ctx, env, service)

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
	//for k, v := range structs.Map(Config.Core) {
	//  key := strings.ToLower(k)
	//	ctx = context.WithValue(ctx, key, v)
	//}
	return ctx
}
