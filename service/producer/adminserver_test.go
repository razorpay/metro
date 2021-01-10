package producer

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/project"
	mocks "github.com/razorpay/metro/internal/project/mocks/core"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestAdminServer_CreateProject(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id",
		Labels:    map[string]string{"foo": "bar"},
	}
	adminServer := newAdminServer(mockProjectCore)
	ctx := context.Background()
	projectModel, err := project.GetValidatedModel(ctx, projectProto, false)
	assert.Nil(t, err)
	mockProjectCore.EXPECT().CreateProject(ctx, projectModel)
	p, err := adminServer.CreateProject(ctx, projectProto)
	assert.Equal(t, projectProto, p)
	assert.Nil(t, err)
}
