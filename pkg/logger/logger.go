package logger

import (
	"context"
	"fmt"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type MutexWrap struct {
	lock     sync.Mutex
	disabled bool
}

//ZapLogger ... struct for handling the logger and associated methods. Kept public to ensure it can be accessible from tests
type ZapLogger struct {

	// internal zap squared logger object
	sugaredLogger *zap.SugaredLogger
	// Used to sync writing to the log. Locking is enabled by Default
	mu MutexWrap
	// Reusable empty entry
	entryPool sync.Pool
}

//Config ... largely kept for extensions and other hooks
type Config struct {
	//LogLevel ... the default log level
	LogLevel string
	//SentryDSN ... sentry DSN in case you want sentry integration
	SentryDSN string
	//SentryEnabled ...enable only for non dev environment. For others, keep it enabled
	SentryEnabled bool
	//SentryLogLevel ... minimum log level for sentry to fire alerts.
	SentryLogLevel string
	//ContextString ... in case of logging extra context information to logs, this should be enabled
	ContextString string
}

var (
	logOnce sync.Once

	config *Config

	logger *ZapLogger

	// Defines the key when adding errors using WithError.
	ErrorKey = "error"
)

//Fields Type to pass when we want to call WithFields for structured logging
//var DefaultFields = make(map[string]interface{}, 0)
//var mutex = & sync.RWMutex{}
const (
	//Debug has verbose message
	Debug = "debug"
	//Info is default log level
	Info = "info"
	//Warn is for logging messages about possible issues
	Warn = "warn"
	//Error is for logging errors
	Error = "error"
	//Fatal is for logging fatal messages. The sytem shutsdown after logging the message.
	Fatal = "fatal"
	//TODO: Should we add panic and DPanic like what zap uses internally?
	//DefaultContextString is for adding extra context
	DefaultContextString = "extra"
	//DefaultFieldConstant for adding existing context
	DefaultFieldConstant = "context"
	//Unique Identifier for context key
	LoggerCtxKey = "Logger"
)

func (mw *MutexWrap) Lock() {
	if !mw.disabled {
		mw.lock.Lock()
	}
}

func (mw *MutexWrap) Unlock() {
	if !mw.disabled {
		mw.lock.Unlock()
	}
}

func (mw *MutexWrap) Disable() {
	mw.disabled = true
}

func (logger *ZapLogger) newEntry() *Entry {
	entry, ok := logger.entryPool.Get().(*Entry)
	if ok {
		return entry
	}
	return NewEntry(logger)
}

func (logger *ZapLogger) releaseEntry(entry *Entry) {
	entry.Data = map[string]interface{}{}
	logger.entryPool.Put(entry)
}

//getZapLevel internal method for reading the default log level
func getZapLevel(level string) zapcore.Level {
	switch level {
	case Info:
		return zapcore.InfoLevel
	case Warn:
		return zapcore.WarnLevel
	case Debug:
		return zapcore.DebugLevel
	case Error:
		return zapcore.ErrorLevel
	case Fatal:
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

//getConfig internal method for reading zap configuration
func getConfig(level zapcore.Level) zap.Config {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(level),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey: "message",

			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalLevelEncoder,

			TimeKey:    "time",
			EncodeTime: zapcore.ISO8601TimeEncoder,

			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}
	return cfg
}

// NewLogger sets up an instance of the logger or returns error if unable to
func NewLogger(c Config) (*ZapLogger, error) {
	var err error
	config = &c
	logOnce.Do(func() {
		if config.ContextString == "" {
			config.ContextString = DefaultContextString
		}
		cores := []zapcore.Core{}
		level := getZapLevel(c.LogLevel)
		cfg := getConfig(level)
		_, err = cfg.Build()
		if err == nil {
			// get a new core with json encoding and append it
			core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg.EncoderConfig), zapcore.AddSync(os.Stderr), level)
			cores = append(cores, core)
			combinedCore := zapcore.NewTee(cores...)

			// AddCallerSkip skips 2 number of callers, this is important else the file that gets
			// logged will always be the wrapped file. In our case zap.go
			l := zap.New(combinedCore,
				zap.AddCallerSkip(2),
				zap.AddCaller(),
			).Sugar()
			defer l.Sync()
			logger = &ZapLogger{sugaredLogger: l}
		}

	})
	if err != nil {
		fmt.Printf("Error getting logger:%s", err.Error())
		return nil, err
	}
	return logger, nil
}

// Adds a field to the log entry, note that it doesn't log until you call
// Debug, Print, Info, Warn, Error, Fatal or Panic. It only creates a log entry.
// If you want multiple fields, use `WithFields`.
func (logger *ZapLogger) WithField(key string, value interface{}) *Entry {
	entry := logger.newEntry()
	defer logger.releaseEntry(entry)
	return entry.WithField(key, value)
}

// WithFields takes default extra context that needs to be attached to
// every other log message and adds it as part of a default map
// from where it is added through every through other log method
// like Debug, Info etc
func (logger *ZapLogger) WithFields(fields map[string]interface{}) *Entry {
	entry := logger.newEntry()
	defer logger.releaseEntry(entry)
	return entry.WithFields(fields)
}

// WithContext returns an instance of the logger with the supplied context populated
func (logger *ZapLogger) WithContext(ctx context.Context, ctxFields []string) *Entry {
	entry := logger.newEntry()
	defer logger.releaseEntry(entry)
	return entry.WithContext(ctx, ctxFields)
}

//WithError adds an error as single field (using the key defined in ErrorKey) to the Entry.
func (logger *ZapLogger) WithError(err error) *Entry {
	entry := logger.newEntry()
	defer logger.releaseEntry(entry)
	return entry.WithError(err)
}

// Ctx gets logger instance from context if available else creates
// a new one basic on the existing config and returns a logger object or error
func Ctx(ctx context.Context) (*Entry, error) {
	k, ok := ctx.Value(LoggerCtxKey).(*Entry)
	if ok {
		return k, nil
	}
	l, err := NewLogger(*config)
	if err != nil {
		return nil, err
	}
	return l.WithContext(ctx, nil), nil
}

func (l *ZapLogger) logw(method string, format string, args map[string]interface{}) {
	entry := l.newEntry()
	defer l.releaseEntry(entry)
	switch method {
	case "debug":
		entry.Debugw(format, args)
	case "info":
		entry.Infow(format, args)
	case "warn":
		entry.Warnw(format, args)
	case "fatal":
		entry.Fatalw(format, args)
	case "panic":
		entry.Panicw(format, args)
	}
}

func (l *ZapLogger) log(method string, format string) {
	entry := l.newEntry()
	defer l.releaseEntry(entry)
	switch method {
	case "debug":
		entry.Debug(format)
	case "info":
		entry.Info(format)
	case "warn":
		entry.Warn(format)
	case "fatal":
		entry.Fatal(format)
	case "panic":
		entry.Panic(format)
	}
}

// Wrapper Functions over entry
func (l *ZapLogger) Debugw(format string, args map[string]interface{}) {
	l.logw("debug", format, args)
}

func (l *ZapLogger) Debug(format string) {
	l.log("debug", format)
}

func (l *ZapLogger) Infow(format string, args map[string]interface{}) {
	l.logw("info", format, args)
}

func (l *ZapLogger) Info(format string) {
	l.log("info", format)
}

func (l *ZapLogger) Warnw(format string, args map[string]interface{}) {
	l.logw("warn", format, args)
}

func (l *ZapLogger) Warn(format string) {
	l.log("warn", format)
}

func (l *ZapLogger) Fatalw(format string, args map[string]interface{}) {
	l.logw("fatal", format, args)
}

func (l *ZapLogger) Fatal(format string) {
	l.log("fatal", format)
}

func (l *ZapLogger) Panicw(format string, args map[string]interface{}) {
	l.logw("panic", format, args)
}

func (l *ZapLogger) Panic(format string) {
	l.log("panic", format)
}
