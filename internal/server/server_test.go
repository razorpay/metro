// +build unit

package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_TraceMethod(t *testing.T) {
	ctx := context.Background()
	trace := shouldEnableTrace(ctx, "/google.pubsub.v1.StatusCheckAPI/LivenessCheck")
	assert.False(t, trace)
	trace = shouldEnableTrace(ctx, "/google.pubsub.v1.StatusCheckAPI/ReadinessCheck")
	assert.False(t, trace)
}
