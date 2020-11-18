package logger_test

import (
	"testing"

	"github.com/razorpay/metro/pkg/logger"
)

func BenchmarkNewLogger(b *testing.B) {
	config := logger.Config{
		LogLevel:       logger.Debug,
		SentryDSN:      "",
		SentryEnabled:  false,
		SentryLogLevel: "",
	}
	lgr, err := logger.NewLogger(config)
	if err != nil {
		b.Errorf("NewLogger :%v", err)
	}
	/*zt := reflect.TypeOf(&(logger.ZapLogger{}))
	if !reflect.DeepEqual(lg, zt) {
		t.Errorf("NewLogger() = %v, want %v", lg, zt)
	}*/
	defaultLogger := lgr.WithFields(map[string]interface{}{"key1": "value1"})
	for n := 0; n < 1000; n++ {
		b.Run("defaultlogger", func(b *testing.B) {
			defaultLogger.Debug("I have the default values for Key1 and Value1")
		})
	}
}

func LogTest(b *testing.B) {
	config := logger.Config{
		LogLevel:       logger.Debug,
		SentryDSN:      "",
		SentryEnabled:  false,
		SentryLogLevel: "",
	}
	lgr, err := logger.NewLogger(config)
	if err != nil {
		b.Errorf("NewLogger :%v", err)
	}
	defaultLogger := lgr.WithFields(map[string]interface{}{"key1": "value1"})
	for n := 0; n < b.N; n++ {
		defaultLogger.Debug("I have the default values for Key1 and Value1")
	}
}
