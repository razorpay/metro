package logger

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// CtxKeyType defines the ctx key type
type CtxKeyType string

// CtxKey is a unique identifier for the context key.
const CtxKey CtxKeyType = "Logger"

var (
	// Log holds an instance of the sugared logger
	Log       *zap.SugaredLogger
	logonce   sync.Once
	hookMutex sync.Mutex
)

// NewLogger sets up an instance of the sugared zap logging driver
func NewLogger(env string, serviceKV map[string]interface{}, hookCore zapcore.Core) (*zap.SugaredLogger, error) {
	var err error
	var slogger *zap.Logger
	logonce.Do(func() {
		// Set-up logger based on env
		switch env {
		case "stage", "prod", "dev_docker", "perf", "func":
			// Logger that writes InfoLevel and above logs to standard error as JSON.
			// It uses a JSON encoder, writes to standard error, and enables sampling.
			// Stacktraces are automatically included on logs of ErrorLevel and above.
			config := zap.NewProductionConfig()
			config.Sampling = nil
			slogger, err = config.Build()
			if err != nil {
				fmt.Printf("Error initializing sugared zap logger: %s", err)
			}
		default:
			// Logger that writes DebugLevel and above logs to standard error in a human-friendly format.
			// It enables development mode (which makes DPanicLevel logs panic), uses a console encoder,
			// writes to standard error, and disables sampling.
			// Stacktraces are automatically included on logs of WarnLevel and above.
			slogger, err = zap.NewDevelopment()
			if err != nil {
				fmt.Printf("Error initializing sugared zap logger: %s", err)
			}
		}
		if hookCore != nil {
			slogger = slogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
				return zapcore.NewTee(core, hookCore)
			}))
		}
		Log = slogger.Sugar()

		AppendServiceKV(serviceKV)
	})

	return Log, err
}

// AppendServiceKV attaches service's core information which should exist in each log.
func AppendServiceKV(serviceKV map[string]interface{}) {
	// serviceKV is service's core information which should exist in each log.
	if serviceKV != nil {
		args := MapToSliceOfKV(serviceKV)
		Log = Log.With(args...)
	}
}

// WithContext returns an instance of the logger with the supplied context populated
func WithContext(ctx context.Context, ctxFields []CtxKeyType) *zap.SugaredLogger {
	if Log == nil {
		// TODO: Valid environment must be passed or else this is invalid invocation.
		NewLogger("", nil, nil)
	}

	if ctx != nil && len(ctxFields) > 0 {
		var args []interface{}
		for _, field := range ctxFields {
			val := ctx.Value(field)
			args = append(args, string(field), val)
		}
		// We receive an array of keys and values, we unpack them here
		newLogger := Log.With(args...)
		return newLogger
	}

	return Log
}

// Ctx gets logger instance from context if available else returns default.
func Ctx(ctx context.Context) *zap.SugaredLogger {
	l, ok := ctx.Value(CtxKey).(*zap.SugaredLogger)
	if ok {
		return l
	}
	return WithContext(ctx, nil)
}

// MapToSliceOfKV ...
func MapToSliceOfKV(m map[string]interface{}) []interface{} {
	s := []interface{}{}
	for k, v := range m {
		s = append(s, k)
		s = append(s, v)
	}
	return s
}
