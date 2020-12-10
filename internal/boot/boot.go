package boot

import (
	"context"
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
	Config config.Config
	// Tracer is used for creating spans for distributed tracing
	Tracer opentracing.Tracer
	// Closer holds an instance to the RequestTracing object's Closer.
	Closer io.Closer
)

func init() {
	// Init config
	err := config_reader.NewDefaultConfig().Load(GetEnv(), &Config)
	if err != nil {
		log.Fatal(err)
	}

	/*
		queues, err := queue.New(&Config.Queue)
		if err != nil {
			log.Fatal(err.Error())
		}
	*/

	//Worker = worker.NewManager(&Config.Worker, queues, Logger(context.Background()))
}

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
func initialize(ctx context.Context, env string) error {
	// Initializes Sentry monitoring client.
	sentry, err := sentrypkg.InitSentry(Config.Sentry, env)
	if err != nil {
		return err
	}
	// Initializes logging driver.
	servicekv := map[string]interface{}{
		"appEnv":        Config.App.Env,
		"serviceName":   Config.App.ServiceName,
		"gitCommitHash": Config.App.GitCommitHash,
	}
	logger, err := logpkg.NewLogger(env, servicekv, sentry)
	if err != nil {
		return err
	}
	Tracer, Closer, err = tracing.Init(Config.Tracing, logger.Desugar())
	if err != nil {
		return err
	}

	return nil
}

// InitMetro initialised all singletons for the application
func InitMetro(ctx context.Context, env string) error {
	err := initialize(ctx, env)
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
