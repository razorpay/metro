package boot

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/internal/constants/contextkeys"
	config_reader "github.com/razorpay/metro/pkg/config"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/spine/db"
	"github.com/razorpay/metro/pkg/tracing"
	"github.com/razorpay/metro/pkg/worker"
	"github.com/razorpay/metro/pkg/worker/queue"
	"github.com/rs/xid"
	otgorm "github.com/smacker/opentracing-gorm"
)

const (
	requestIDHttpHeaderKey = "X-Request-ID"
	requestIDCtxKey        = "RequestID"
)

var (
	// Config contains application configuration values.
	Config config.Config

	// DB holds the application db connection.
	DB *db.DB

	Tracer opentracing.Tracer
	Worker worker.IManager
)

func init() {
	// Init config
	err := config_reader.NewDefaultConfig().Load(GetEnv(), &Config)
	if err != nil {
		log.Fatal(err)
	}

	InitLogger(context.Background())

	// Init Db
	DB, err = db.NewDb(&Config.Db)
	if err != nil {
		log.Fatal(err.Error())
	}

	queues, err := queue.New(&Config.Queue)
	if err != nil {
		log.Fatal(err.Error())
	}

	Worker = worker.NewManager(&Config.Worker, queues, Logger(context.Background()))
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
	log := InitLogger(ctx)

	context.WithValue(ctx, logger.LoggerCtxKey, log)

	// Puts git commit hash into config.
	// This is not read automatically because env variable is not in expected format.
	Config.App.GitCommitHash = os.Getenv("GIT_COMMIT_HASH")

	otgorm.AddGormCallbacks(DB.Instance(ctx))

	// Register DB stats prometheus collector
	collector := sqlstats.NewStatsCollector(Config.Db.URL+"-"+Config.Db.Name, DB.Instance(ctx).DB())
	prometheus.MustRegister(collector)

	return nil
}

func InitApi(ctx context.Context, env string) error {
	err := initialize(ctx, env)
	if err != nil {
		return err
	}

	return nil
}

func InitMigration(ctx context.Context, env string) error {
	err := initialize(ctx, env)
	if err != nil {
		return err
	}

	return nil
}

// InitTracing initialises opentracing exporter
func InitTracing(ctx context.Context) (io.Closer, error) {
	t, closer, err := tracing.Init(Config.Tracing, Logger(ctx))

	Tracer = t

	return closer, err
}

func InitWorker(ctx context.Context) error {
	return Worker.Start(ctx)
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

func Logger(ctx context.Context) *logger.Entry {
	ctxLogger, err := logger.Ctx(ctx)

	if err == nil {
		return ctxLogger
	}

	return nil
}

func InitLogger(ctx context.Context) *logger.ZapLogger {
	lgrConfig := logger.Config{
		LogLevel:       logger.Debug,
		SentryDSN:      Config.Sentry.DNS,
		SentryEnabled:  Config.Sentry.Enabled,
		SentryLogLevel: Config.Sentry.LogLevel,
		ContextString:  "metro",
	}

	Logger, err := logger.NewLogger(lgrConfig)

	if err != nil {
		panic("failed to initialize logger")
	}

	return Logger
}
