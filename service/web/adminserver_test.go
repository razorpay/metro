// +build unit

package web

import (
	"context"
	"fmt"
	"testing"

	"github.com/razorpay/metro/internal/credentials"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/golang/mock/gomock"
	mocks4 "github.com/razorpay/metro/internal/credentials/mocks/core"
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
	mockTopicCore := mocks3.NewMockICore(ctrl)
	mockCredentialsCore := mocks4.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id",
		Labels:    map[string]string{"foo": "bar"},
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, mockCredentialsCore, nil)
	ctx := context.Background()
	projectModel, err := project.GetValidatedModelForCreate(ctx, projectProto)
	assert.Nil(t, err)
	mockProjectCore.EXPECT().CreateProject(gomock.Any(), projectModel)
	p, err := adminServer.CreateProject(ctx, projectProto)
	assert.Equal(t, projectProto, p)
	assert.Nil(t, err)
}

func TestAdminServer_CreateProjectValidationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCOre := mocks3.NewMockICore(ctrl)
	mockCredentialsCore := mocks4.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id-",
		Labels:    map[string]string{"foo": "bar"},
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCOre, mockCredentialsCore, nil)
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
	mockCredentialsCore := mocks4.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id",
		Labels:    map[string]string{"foo": "bar"},
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCOre, mockCredentialsCore, nil)
	ctx := context.Background()
	projectModel, err := project.GetValidatedModelForCreate(ctx, projectProto)
	assert.Nil(t, err)
	mockProjectCore.EXPECT().CreateProject(gomock.Any(), projectModel).Times(1).Return(fmt.Errorf("error"))
	p, err := adminServer.CreateProject(ctx, projectProto)
	assert.Nil(t, p)
	assert.NotNil(t, err)
}

func TestAdminServer_DeleteProject(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)
	mockCredentialsCore := mocks4.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		ProjectId: "test-project-id",
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, mockCredentialsCore, nil)
	ctx := context.Background()
	projectModel, err := project.GetValidatedModelForDelete(ctx, projectProto)
	assert.Nil(t, err)
	mockProjectCore.EXPECT().DeleteProject(gomock.Any(), projectModel)
	mockTopicCore.EXPECT().DeleteProjectTopics(gomock.Any(), projectModel.ProjectID).Times(1).Return(nil)
	mockSubscriptionCore.EXPECT().DeleteProjectSubscriptions(gomock.Any(), projectModel.ProjectID).Times(1).Return(nil)

	p, err := adminServer.DeleteProject(ctx, projectProto)
	assert.Equal(t, &emptypb.Empty{}, p)
	assert.Nil(t, err)
}

func TestAdminServer_DeleteProjectValidationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)
	mockCredentialsCore := mocks4.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		ProjectId: "",
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}
	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, mockCredentialsCore, nil)
	ctx := context.Background()

	p, err := adminServer.DeleteProject(ctx, projectProto)
	assert.NotNil(t, err)
	assert.Nil(t, p)
}
