// +build unit

package web

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/golang/mock/gomock"
	mocks4 "github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/project"
	mocks "github.com/razorpay/metro/internal/project/mocks/core"
	mocks2 "github.com/razorpay/metro/internal/subscription/mocks/core"
	"github.com/razorpay/metro/internal/topic"
	mocks3 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/messagebroker"
	mocks5 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	adminv1 "github.com/razorpay/metro/rpc/admin/v1"
	metrov1 "github.com/razorpay/metro/rpc/pubsub/v1"
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
	adminServer := newAdminServer(mockProjectCore, mockSubscriptionCore, mockTopicCOre, nil)
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
	adminServer := newAdminServer(mockProjectCore, mockSubscriptionCore, mockTopicCOre, nil)
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
	adminServer := newAdminServer(mockProjectCore, mockSubscriptionCore, mockTopicCOre, nil)
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
	adminServer := newAdminServer(mockProjectCore, mockSubscriptionCore, mockTopicCore, nil)
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
	adminServer := newAdminServer(mockProjectCore, mockSubscriptionCore, mockTopicCore, nil)
	ctx := context.Background()

	p, err := adminServer.DeleteProject(ctx, projectProto)
	assert.NotNil(t, err)
	assert.Nil(t, p)
}

func TestAdminServer_CreateAdminTopicSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)
	mockBrokerStore := mocks4.NewMockIBrokerStore(ctrl)
	admin := mocks5.NewMockAdmin(ctrl)

	server := newAdminServer(mockProjectCore, mockSubscriptionCore, mockTopicCore, mockBrokerStore)

	ctx := context.Background()
	req := &adminv1.AdminTopic{
		Name:          "projects/project123/topics/test-topic",
		Labels:        map[string]string{"foo": "bar"},
		NumPartitions: 2,
	}

	topicModel, err := topic.GetValidatedAdminModel(ctx, req)
	assert.Nil(t, err)

	mockTopicCore.EXPECT().CreateTopic(ctx, topicModel).Times(1).Return(nil)
	mockBrokerStore.EXPECT().GetAdmin(ctx, gomock.Any()).Times(1).Return(admin, nil)
	admin.EXPECT().CreateTopic(ctx, gomock.Any()).Times(1).Return(messagebroker.CreateTopicResponse{}, nil)

	tp, err := server.CreateTopic(ctx, req)
	assert.Equal(t, req, tp)
	assert.Nil(t, err)
}

func TestAdminServer_CreateAdminTopicFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)

	server := newAdminServer(mockProjectCore, mockSubscriptionCore, mockTopicCore, nil)

	ctx := context.Background()
	req := &adminv1.AdminTopic{
		Name:          "projects/project123/topics/test-topic",
		Labels:        map[string]string{"foo": "bar"},
		NumPartitions: 5,
	}

	topicModel, err := topic.GetValidatedAdminModel(ctx, req)
	assert.Nil(t, err)

	mockTopicCore.EXPECT().CreateTopic(ctx, topicModel).Times(1).Return(fmt.Errorf("error"))

	tp, err := server.CreateTopic(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, tp)
}
