//go:build unit
// +build unit

package web

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	brokerStoreMock "github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/credentials"
	mocks4 "github.com/razorpay/metro/internal/credentials/mocks/core"
	mocksnb "github.com/razorpay/metro/internal/nodebinding/mocks/core"
	"github.com/razorpay/metro/internal/project"
	mocks "github.com/razorpay/metro/internal/project/mocks/core"
	mocks2 "github.com/razorpay/metro/internal/subscription/mocks/core"
	"github.com/razorpay/metro/internal/topic"
	mocks3 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/encryption"
	"github.com/razorpay/metro/pkg/messagebroker"
	messagebrokermock "github.com/razorpay/metro/pkg/messagebroker/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestAdminServer_CreateProject(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)
	mockCredentialsCore := mocks4.NewMockICore(ctrl)
	mocknodeBindingCore := mocksnb.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id",
		Labels:    map[string]string{"foo": "bar"},
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, mockCredentialsCore, mocknodeBindingCore, nil)
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
	mocknodeBindingCore := mocksnb.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id-",
		Labels:    map[string]string{"foo": "bar"},
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCOre, mockCredentialsCore, mocknodeBindingCore, nil)
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
	mocknodeBindingCore := mocksnb.NewMockICore(ctrl)

	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id",
		Labels:    map[string]string{"foo": "bar"},
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCOre, mockCredentialsCore, mocknodeBindingCore, nil)
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
	mocknodeBindingCore := mocksnb.NewMockICore(ctrl)
	projectProto := &metrov1.Project{
		ProjectId: "test-project-id",
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, mockCredentialsCore, mocknodeBindingCore, nil)
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
	mocknodeBindingCore := mocksnb.NewMockICore(ctrl)
	projectProto := &metrov1.Project{
		ProjectId: "",
	}

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}
	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, mockCredentialsCore, mocknodeBindingCore, nil)
	ctx := context.Background()

	p, err := adminServer.DeleteProject(ctx, projectProto)
	assert.NotNil(t, err)
	assert.Nil(t, p)
}

func TestAdminServer_GetProjectCredentials(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)
	mockCredentialsCore := mocks4.NewMockICore(ctrl)
	mocknodeBindingCore := mocksnb.NewMockICore(ctrl)
	projectCredProto := &metrov1.ProjectCredentials{
		ProjectId: "test-project",
		Username:  "test-project__00f790",
	}

	encryption.RegisterEncryptionKey("key")
	pwd, _ := encryption.EncryptAsHexString([]byte("password"))

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	credential := &credentials.Model{
		ProjectID: "test-project",
		Username:  "test-project__00f790",
		Password:  pwd,
	}

	expectedCredProto := &metrov1.ProjectCredentials{
		ProjectId: "test-project",
		Username:  "test-project__00f790",
		Password:  "password",
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, mockCredentialsCore, mocknodeBindingCore, nil)
	ctx := context.Background()

	mockCredentialsCore.EXPECT().
		Get(ctx, projectCredProto.ProjectId, projectCredProto.Username).Times(1).
		Return(credential, nil)
	p, err := adminServer.GetProjectCredentials(ctx, projectCredProto)
	assert.Nil(t, err)
	assert.Equal(t, expectedCredProto, p)
}

func TestAdminServer_ListProjectCredentials(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)
	mockCredentialsCore := mocks4.NewMockICore(ctrl)
	mocknodeBindingCore := mocksnb.NewMockICore(ctrl)
	projectCredProto := &metrov1.ProjectCredentials{
		ProjectId: "test-project",
		Username:  "test-project__00f790",
	}

	encryption.RegisterEncryptionKey("key")
	pwd, _ := encryption.EncryptAsHexString([]byte("password"))

	admin := &credentials.Model{
		Username: "u",
		Password: "p",
	}

	credential := []*credentials.Model{{
		ProjectID: "test-project",
		Username:  "test-project__00f790",
		Password:  pwd,
	}}

	expectedCredProto := &metrov1.ProjectCredentialsList{
		ProjectCredentials: []*metrov1.ProjectCredentials{{
			ProjectId: "test-project",
			Username:  "test-project__00f790",
			Password:  credentials.AsteriskString + "word",
		}},
	}

	adminServer := newAdminServer(admin, mockProjectCore, mockSubscriptionCore, mockTopicCore, mockCredentialsCore, mocknodeBindingCore, nil)
	ctx := context.Background()

	mockCredentialsCore.EXPECT().
		List(ctx, projectCredProto.ProjectId).Times(1).
		Return(credential, nil)
	p, err := adminServer.ListProjectCredentials(ctx, projectCredProto)
	assert.Nil(t, err)
	assert.Equal(t, expectedCredProto, p)
}

func Test_adminServer_CreateProjectCredentials(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCredentialsCore := mocks4.NewMockICore(ctrl)
	req := &metrov1.ProjectCredentials{ProjectId: "test-project"}

	tests := []struct {
		name    string
		wantErr bool
		err     error
	}{
		{
			name:    "Create Project credentials without error",
			wantErr: false,
			err:     nil,
		},
		{
			name:    "Create Project credentials with error",
			wantErr: true,
			err:     fmt.Errorf("Something went wrong"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockCredentialsCore.EXPECT().Create(ctx, gomock.Any()).Times(1).Return(test.err)
			adminServer := newAdminServer(
				&credentials.Model{Username: "u", Password: "p"},
				mocks.NewMockICore(ctrl),
				mocks2.NewMockICore(ctrl),
				mocks3.NewMockICore(ctrl),
				mockCredentialsCore,
				mocksnb.NewMockICore(ctrl),
				nil,
			)
			got, err := adminServer.CreateProjectCredentials(ctx, req)
			assert.Equal(t, test.wantErr, err != nil)
			assert.Equal(t, !test.wantErr, got != nil)
		})
	}
}

func Test_adminServer_DeleteProjectCredentials(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockCredentialsCore := mocks4.NewMockICore(ctrl)
	credentialModel := getDummyCredentialModel()

	req := &metrov1.ProjectCredentials{
		ProjectId: "test-project",
		Username:  "test-project__00f790",
	}

	tests := []struct {
		name            string
		credentialModel *credentials.Model
		getErr          error
		deleteErr       error
		wantErr         bool
	}{
		{
			name:            "Delete Project Credentials without error",
			credentialModel: credentialModel,
			getErr:          nil,
			deleteErr:       nil,
			wantErr:         false,
		},
		{
			name:            "Delete Project Credentials which doesn't exist",
			credentialModel: nil,
			getErr:          fmt.Errorf("Something went wrong"),
			deleteErr:       fmt.Errorf("Something went wrong"),
			wantErr:         true,
		},
		{
			name:            "Delete Project Credentials with error",
			credentialModel: credentialModel,
			getErr:          nil,
			deleteErr:       fmt.Errorf("Something went wrong"),
			wantErr:         true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockCredentialsCore.EXPECT().Get(ctx, req.ProjectId, req.Username).Return(test.credentialModel, test.getErr)
			mockCredentialsCore.EXPECT().Delete(ctx, credentialModel).Return(test.deleteErr).MaxTimes(1)
			adminServer := newAdminServer(
				&credentials.Model{Username: "u", Password: "p"},
				mocks.NewMockICore(ctrl),
				mocks2.NewMockICore(ctrl),
				mocks3.NewMockICore(ctrl),
				mockCredentialsCore,
				mocksnb.NewMockICore(ctrl),
				nil,
			)
			_, err := adminServer.DeleteProjectCredentials(ctx, req)
			assert.Equal(t, test.wantErr, err != nil)
		})
	}
}

func Test_adminServer_ModifyTopic(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	topicModel := &topic.Model{
		Name:               "projects/test-project/topics/test-topic",
		NumPartitions:      1,
		ExtractedProjectID: "test-project",
		ExtractedTopicName: "test-topic",
	}
	req := &metrov1.AdminTopic{
		Name:          topicModel.Name,
		NumPartitions: int32(topicModel.NumPartitions) + 1,
	}
	updatedModel := *topicModel
	updatedModel.NumPartitions = int(req.GetNumPartitions())

	mockCredentialsCore := mocks4.NewMockICore(ctrl)
	mockSubscriptionCore := mocks2.NewMockICore(ctrl)
	mockSubscriptionCore.EXPECT().RescaleSubTopics(ctx, &updatedModel).Return(nil).AnyTimes()

	mockAdmin := messagebrokermock.NewMockBroker(ctrl)
	mockTopicCore := mocks3.NewMockICore(ctrl)
	mockTopicCore.EXPECT().Get(ctx, req.Name).Return(topicModel, nil).AnyTimes()
	mockTopicCore.EXPECT().UpdateTopic(ctx, &updatedModel).Return(nil).AnyTimes()
	mockTopicCore.EXPECT().Exists(ctx, topicModel.Key()).Return(true, nil).AnyTimes()

	brokerStoreMock := brokerStoreMock.NewMockIBrokerStore(ctrl)
	brokerStoreMock.EXPECT().GetAdmin(gomock.Any(), gomock.Any()).Return(mockAdmin, nil).AnyTimes()

	tests := []struct {
		name    string
		wantErr bool
		err     error
	}{
		{
			name:    "Modify topic without error",
			wantErr: false,
			err:     nil,
		},
		{
			name:    "Modify topic with error",
			wantErr: true,
			err:     fmt.Errorf("Something went wrong"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockAdmin.EXPECT().AddTopicPartitions(ctx, messagebroker.AddTopicPartitionRequest{
				Name:          req.GetName(),
				NumPartitions: int(req.GetNumPartitions()),
			}).Return(&messagebroker.AddTopicPartitionResponse{}, test.err).Times(1)
			adminServer := newAdminServer(
				&credentials.Model{Username: "u", Password: "p"},
				mocks.NewMockICore(ctrl),
				mockSubscriptionCore,
				mockTopicCore,
				mockCredentialsCore,
				mocksnb.NewMockICore(ctrl),
				brokerStoreMock,
			)
			_, err := adminServer.ModifyTopic(ctx, req)
			assert.Equal(t, test.wantErr, err != nil)
		})
	}
}

func Test_adminServer_MigrateSubscriptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mocknodeBindingCore := mocksnb.NewMockICore(ctrl)

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "Migrate Subscriptions without error",
			err:  nil,
		},
		{
			name: "Migrate Subscriptions with error",
			err:  fmt.Errorf("Something went wrong"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mocknodeBindingCore.EXPECT().TriggerNodeBindingRefresh(ctx).Return(test.err)
			adminServer := newAdminServer(
				&credentials.Model{Username: "u", Password: "p"},
				mocks.NewMockICore(ctrl),
				mocks2.NewMockICore(ctrl),
				mocks3.NewMockICore(ctrl),
				mocks4.NewMockICore(ctrl),
				mocknodeBindingCore,
				nil,
			)
			_, err := adminServer.MigrateSubscriptions(ctx, &metrov1.Subscriptions{})
			assert.Equal(t, test.err != nil, err != nil)
		})
	}
}

func getDummyCredentialModel() *credentials.Model {
	encryption.RegisterEncryptionKey("key")
	pwd, _ := encryption.EncryptAsHexString([]byte("password"))
	return &credentials.Model{
		ProjectID: "test-project",
		Username:  "test-project__00f790",
		Password:  pwd,
	}
}
