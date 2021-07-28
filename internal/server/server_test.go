package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_TraceMethod(t *testing.T) {
	ctx := context.Background()
	trace := traceMethod(ctx, "/google.pubsub.v1.StatusCheckAPI/LivenessCheck")
	assert.False(t, trace)
	trace = traceMethod(ctx, "/google.pubsub.v1.StatusCheckAPI/ReadinessCheck")
	assert.False(t, trace)
}
