// +build unit

package web

import (
	"context"
	"fmt"
	"testing"

	"github.com/razorpay/metro/internal/auth"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/project"
	mocks "github.com/razorpay/metro/internal/project/mocks/core"
	mocks2 "github.com/razorpay/metro/internal/subscription/mocks/core"
	mocks3 "github.com/razorpay/metro/internal/topic/mocks/core"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestAdminServer_CreateProject(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCOre := mocks3.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id",
		Labels:    map[string]string{"foo": "bar"},
	}

	admin := &auth.Auth{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCOre, nil)
	ctx := context.Background()
	projectModel, err := project.GetValidatedModelForCreate(ctx, projectProto)
	assert.Nil(t, err)
	mockProjectCore.EXPECT().CreateProject(ctx, projectModel)
	p, err := adminServer.CreateProject(ctx, projectProto)
	assert.Equal(t, projectProto, p)
	assert.Nil(t, err)
}

func TestAdminServer_CreateProjectValidationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCOre := mocks3.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id-",
		Labels:    map[string]string{"foo": "bar"},
	}

	admin := &auth.Auth{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCOre, nil)
	ctx := context.Background()
	p, err := adminServer.CreateProject(ctx, projectProto)
	assert.Nil(t, p)
	assert.NotNil(t, err)
}

func TestAdminServer_CreateProjectFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCOre := mocks3.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id",
		Labels:    map[string]string{"foo": "bar"},
	}

	admin := &auth.Auth{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCOre, nil)
	ctx := context.Background()
	projectModel, err := project.GetValidatedModelForCreate(ctx, projectProto)
	assert.Nil(t, err)
	mockProjectCore.EXPECT().CreateProject(ctx, projectModel).Times(1).Return(fmt.Errorf("error"))
	p, err := adminServer.CreateProject(ctx, projectProto)
	assert.Nil(t, p)
	assert.NotNil(t, err)
}

func TestAdminServer_DeleteProject(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		ProjectId: "test-project-id",
	}

	admin := &auth.Auth{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, nil)
	ctx := context.Background()
	projectModel, err := project.GetValidatedModelForDelete(ctx, projectProto)
	assert.Nil(t, err)
	mockProjectCore.EXPECT().DeleteProject(ctx, projectModel)
	mockTopicCore.EXPECT().DeleteProjectTopics(ctx, projectModel.ProjectID).Times(1).Return(nil)
	mockSubscriptionCore.EXPECT().DeleteProjectSubscriptions(ctx, projectModel.ProjectID).Times(1).Return(nil)

	p, err := adminServer.DeleteProject(ctx, projectProto)
	assert.Equal(t, &emptypb.Empty{}, p)
	assert.Nil(t, err)
}

func TestAdminServer_DeleteProjectValidationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		ProjectId: "",
	}

	admin := &auth.Auth{
		Username: "u",
		Password: "p",
	}
	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, nil)
	ctx := context.Background()

	p, err := adminServer.DeleteProject(ctx, projectProto)
	assert.NotNil(t, err)
	assert.Nil(t, p)
}
