package logger_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestEntryWithError(t *testing.T) {

	assert := assert.New(t)

	defer func() {
		logger.ErrorKey = "error"
	}()

	err := fmt.Errorf("doomed here %d", 1234)

	config := logger.Config{
		LogLevel:       logger.Debug,
		SentryDSN:      "",
		SentryEnabled:  false,
		SentryLogLevel: "",
	}

	lgr, lErr := logger.NewLogger(config)

	assert.Nil(lErr)

	entry := logger.NewEntry(lgr)

	assert.Equal(err.Error(), entry.WithError(err).Data["error"])

	logger.ErrorKey = "err"
	assert.Equal(err.Error(), entry.WithError(err).Data["err"])

}

func TestEntryWithContext(t *testing.T) {
	assert := assert.New(t)
	ctx := context.WithValue(context.Background(), "foo", "bar")

	config := logger.Config{
		LogLevel:       logger.Debug,
		SentryDSN:      "",
		SentryEnabled:  false,
		SentryLogLevel: "",
	}

	lgr, err := logger.NewLogger(config)

	assert.Nil(err)

	entry := logger.NewEntry(lgr)

	assert.Equal("bar", entry.WithContext(ctx, []string{"foo"}).Data["foo"])
}
