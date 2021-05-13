// +build unit

package boot

import (
	"context"
	"testing"

	"github.com/razorpay/metro/pkg/monitoring/sentry"
	tracingpkg "github.com/razorpay/metro/pkg/tracing"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/config"

	"github.com/stretchr/testify/assert"
)

func TestBoot_NewContext_Nil(t *testing.T) {
	ctx := NewContext(nil)
	assert.NotNil(t, ctx)
}

func TestBoot_NewContext_NotNil(t *testing.T) {
	ctxIn := context.Background()
	ctxOut := NewContext(ctxIn)
	assert.NotNil(t, ctxOut)
	assert.Equal(t, ctxIn, ctxOut)
}

func TestBoot_InitMonitoring(t *testing.T) {
	err := InitMonitoring(
		"test",
		config.App{
			Env:           "test",
			ServiceName:   "metro",
			GitCommitHash: uuid.New().String(),
		},
		sentry.Config{
			Mock:    true,
			AppName: "metro",
		},
		tracingpkg.Config{
			Disabled:    true,
			ServiceName: "metro",
		},
	)

	assert.Nil(t, err)
	assert.NotNil(t, Tracer)
	assert.NotNil(t, Closer)
}
