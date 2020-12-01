package logger

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// LoggerCtxKey is a unique identifier for the context key.
	LoggerCtxKey = "Logger"
)

var (
	// Log holds an instance of the sugared logger
	Log       *zap.SugaredLogger
	logonce   sync.Once
	hookMutex sync.Mutex
)

// NewLogger sets up an instance of the sugared zap logging driver
func NewLogger(env string, serviceKV map[string]interface{}, hookCore zapcore.Core) (*zap.SugaredLogger, error) {

	var err error
	logonce.Do(func() {
		// Set-up logger based on env
		switch env {
		case "stage", "prod", "drone", "perf", "func":
			// Logger that writes InfoLevel and above logs to standard error as JSON.
			// It uses a JSON encoder, writes to standard error, and enables sampling.
			// Stacktraces are automatically included on logs of ErrorLevel and above.
			config := zap.NewProductionConfig()
			config.Sampling = nil
			slogger, err := config.Build()
			if err != nil {
				fmt.Printf("Error initializing sugared zap logger: %s", err)
			}
			slogger = slogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
				return zapcore.NewTee(core, hookCore)
			}))
			Log = slogger.Sugar()
		default:
			// Logger that writes DebugLevel and above logs to standard error in a human-friendly format.
			// It enables development mode (which makes DPanicLevel logs panic), uses a console encoder,
			// writes to standard error, and disables sampling.
			// Stacktraces are automatically included on logs of WarnLevel and above.
			slogger, err := zap.NewDevelopment()
			slogger = slogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
				return zapcore.NewTee(core, hookCore)
			}))
			if err != nil {
				fmt.Printf("Error initializing sugared zap logger: %s", err)
			}
			Log = slogger.Sugar()
		}

		AppendServiceKV(serviceKV)
	})

	return Log, err
}

// serviceKV is service's core information which should exist in each log.
func AppendServiceKV(serviceKV map[string]interface{}) {
	// serviceKV is service's core information which should exist in each log.
	if serviceKV != nil {
		args := MapToSliceOfKV(serviceKV)
		Log = Log.With(args...)
	}
}

// WithContext returns an instance of the logger with the supplied context populated
func WithContext(ctx context.Context, ctxFields []string) *zap.SugaredLogger {
	if Log == nil {
		// TODO: Valid environment must be passed or else this is invalid invocation.
		NewLogger("", nil, nil)
	}

	if ctx != nil && len(ctxFields) > 0 {
		var args []interface{}
		for _, field := range ctxFields {
			val := ctx.Value(field)
			args = append(args, field, val)
		}
		// We receive an array of keys and values, we unpack them here
		newLogger := Log.With(args...)
		return newLogger
	}

	return Log
}

// Ctx gets logger instance from context if available else returns default.
func Ctx(ctx context.Context) *zap.SugaredLogger {
	l, ok := ctx.Value(LoggerCtxKey).(*zap.SugaredLogger)
	if ok {
		return l
	}
	return WithContext(ctx, nil)
}

// TODO: Move to a common place. No utility file please!
func MapToSliceOfKV(m map[string]interface{}) []interface{} {
	s := []interface{}{}
	for k, v := range m {
		s = append(s, k)
		s = append(s, v)
	}
	return s
}

// RegisterHook incrementally adds a Hook to the Logger initialized
// func RegisterHook(hook func(zapcore.Entry) error) {
// 	hookMutex.Lock()
// 	defer hookMutex.Unlock()

// 	dl := Log.Desugar()

// 	dl.WithOptions(
// 		zap.Hooks(hook),
// 	)

// 	Log = dl.Sugar()

// }
