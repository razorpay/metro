package project

import (
	"context"
	"testing"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestValidation_FromProto(t *testing.T) {
	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id",
		Labels:    map[string]string{"foo": "bar"},
	}
	projectModel := fromProto(projectProto)
	assert.Equal(t, projectProto.Name, projectModel.Name)
	assert.Equal(t, projectProto.ProjectId, projectModel.ProjectID)
	assert.Equal(t, projectProto.Labels, projectModel.Labels)
}

func TestValidation_isValidProjectID(t *testing.T) {
	ctx := context.Background()
	ok := isValidProjectID(ctx, "invalid-")
	assert.False(t, ok)
	ok = isValidProjectID(ctx, "invld")
	assert.False(t, ok)
	ok = isValidProjectID(ctx, "invalidID")
	assert.False(t, ok)
	ok = isValidProjectID(ctx, "valid01id")
	assert.True(t, ok)
	ok = isValidProjectID(ctx, "valid-01-id")
	assert.True(t, ok)
}
