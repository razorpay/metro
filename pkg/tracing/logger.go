package tracing

import (
	logger "github.com/razorpay/metro/pkg/logger"
)

// log struct expected by jaeger client logger
type log struct {
	e *logger.Entry
}

// Error implements error reporter required by jaeger client
func (l *log) Error(msg string) {
	l.e.Error(msg)
}

// Infof logs a message at info priority. Required by jaeger client
func (l *log) Infof(msg string, args ...interface{}) {
	l.e.WithField("args", args).Info(msg)
}
