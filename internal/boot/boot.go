package boot

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/internal/constants/contextkeys"
	config_reader "github.com/razorpay/metro/pkg/config"
	logpkg "github.com/razorpay/metro/pkg/logger"
	sentrypkg "github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/tracing"
	"github.com/razorpay/metro/pkg/worker"

	"github.com/rs/xid"
)

const (
	requestIDHttpHeaderKey = "X-Request-ID"
	requestIDCtxKey        = "RequestID"
)

var (
	// Config contains application configuration values.
	Config config.Config

	Tracer opentracing.Tracer
	Worker worker.IManager
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

func GetEnv() string {
	// Fetch env for bootstrapping
	environment := os.Getenv("APP_ENV")
	if environment == "" {
		environment = "dev"
	}

	return environment
}

// GetRequestID gets the request id
// if its already set in the given context
// if there is no requestID set then it'll create a new
// request id and returns the same
func GetRequestID(ctx context.Context) string {
	if val, ok := ctx.Value(contextkeys.RequestID).(string); ok {
		return val
	}
	return xid.New().String()
}

// WithRequestID adds a request if to the context and gives the updated context back
// if the passed requestID is empty then creates one by itself
func WithRequestID(ctx context.Context, requestID string) context.Context {
	if requestID == "" {
		requestID = xid.New().String()
	}

	return context.WithValue(ctx, contextkeys.RequestID, requestID)
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

func InitProducer(ctx context.Context, env string) error {
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
