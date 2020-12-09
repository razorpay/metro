package boot

import (
	"context"
	"fmt"
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

	"github.com/rs/xid"
)

const (
	requestIDHttpHeaderKey = "X-Request-ID"
	requestIDCtxKey        = "RequestID"
)

var (
	// Config contains application configuration values.
	ServiceConfig config.ServiceConfig

	Tracer opentracing.Tracer
	// Closer holds an instance to the RequestTracing object's Closer.
	Closer io.Closer
)

func getEnv() string {
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
func initialize(ctx context.Context, service string) error {
	// get env (dev/stage/prod) and init config
	env := getEnv()

	// read the service config for env
	var appConfig config.Config
	err := config_reader.NewDefaultConfig().Load(env, &appConfig)
	if err != nil {
		log.Fatal(err)
	}

	serviceConfig, ok := appConfig[service]

	if !ok {
		log.Fatal(fmt.Errorf("`%v` service missing config", service))
	}

	ServiceConfig = serviceConfig

	// Initializes Sentry monitoring client.
	sentry, err := sentrypkg.InitSentry(ServiceConfig.Sentry, env)
	if err != nil {
		return err
	}
	// Initializes logging driver.
	servicekv := map[string]interface{}{
		"appEnv":        ServiceConfig.App.Env,
		"serviceName":   ServiceConfig.App.ServiceName,
		"gitCommitHash": ServiceConfig.App.GitCommitHash,
	}
	logger, err := logpkg.NewLogger(env, servicekv, sentry)
	if err != nil {
		return err
	}
	Tracer, Closer, err = tracing.Init(ServiceConfig.Tracing, logger.Desugar())
	if err != nil {
		return err
	}

	return nil
}

func InitMetro(ctx context.Context, service string) error {
	err := initialize(ctx, service)
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
