package sentry

import (
	"fmt"
	"time"

	"github.com/getsentry/sentry-go"
	"go.uber.org/zap/zapcore"
)

var (
	sentryLevel zapcore.Level
	env         string
	appName     string
)

type core struct {
	hook        func(zapcore.Entry) error
	sentryLevel zapcore.Level
	fields      map[string]interface{}
	zapcore.LevelEnabler
}

// Config holds sentry config
type Config struct {
	AppName    string
	DSN        string
	Mock       bool
	ErrorLevel int8
}

// WriteHook captures Error events fired byy the logger
func (c *core) Write(log zapcore.Entry, fs []zapcore.Field) error {
	// Any additional info goes into extra
	extra := make(map[string]interface{})
	extra["Caller"] = log.Caller
	extra["WithFields"] = c.fields

	// Create event to flush logs
	event := &sentry.Event{
		ServerName:  appName,
		Environment: env,
		Level:       sentry.Level(log.Level.String()),
		Message:     log.Message,
		Logger:      log.LoggerName,
		Transaction: log.Stack,
		Extra:       extra,
	}
	sentry.CaptureEvent(event)

	return nil
}

// MockHook is a No-op action
func MockHook(log zapcore.Entry) error {
	return nil
}

// InitSentry initializes Sentry client and returns a Hook top be used by the logger
func InitSentry(conf *Config, environ string) (zapcore.Core, error) {

	sentryCore := zapcore.NewNopCore()
	if conf.Mock == false {
		// TODO: If mock is set to true then skip sentry Init but create zapcore.Core to validate Interface
		err := sentry.Init(sentry.ClientOptions{
			Dsn: conf.DSN,
		})
		if err != nil {
			fmt.Printf("Sentry initialization failed: %v\n", err)
			return nil, err
		}
		env = environ
		appName = conf.AppName

		sentryCore = &core{
			LevelEnabler: zapcore.Level(conf.ErrorLevel),
			sentryLevel:  zapcore.Level(conf.ErrorLevel),
			fields:       make(map[string]interface{}),
		}
	}
	return sentryCore, nil
}

func (c *core) With(fs []zapcore.Field) zapcore.Core {
	return c.with(fs)
}

func (c *core) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if zapcore.WarnLevel.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *core) Sync() error {
	sentry.Flush(time.Second)
	return nil
}

func (c *core) with(fs []zapcore.Field) *core {
	m := make(map[string]interface{}, len(c.fields))
	for k, v := range c.fields {
		m[k] = v
	}

	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fs {
		f.AddTo(enc)
	}

	for k, v := range enc.Fields {
		m[k] = v
	}

	return &core{
		fields:       m,
		LevelEnabler: c.LevelEnabler,
	}
}
