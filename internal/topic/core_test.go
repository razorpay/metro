//go:build unit
// +build unit

package topic

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/brokerstore"
	brokerstoremock "github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/project"
	projectcoremock "github.com/razorpay/metro/internal/project/mocks/core"
	topicrepomock "github.com/razorpay/metro/internal/topic/mocks/repo"
	"github.com/razorpay/metro/pkg/messagebroker"
	messagebrokermock "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"
)

func TestCore_CreateTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	mockAdmin := messagebrokermock.NewMockBroker(ctrl)
	topicCore := NewCore(mockTopicRepo, mockProjectCore, mockBrokerStore)
	ctx := context.Background()
	dTopic := getDummyTopicModel()
	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), dTopic.ExtractedProjectID).Return(true, nil)
	mockBrokerStore.EXPECT().GetAdmin(gomock.Any(), messagebroker.AdminClientOptions{}).Return(mockAdmin, nil)
	mockAdmin.EXPECT().CreateTopic(gomock.Any(), messagebroker.CreateTopicRequest{dTopic.Name, DefaultNumPartitions}).Return(messagebroker.CreateTopicResponse{}, nil)
	mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+"topics/test-project/test-topic")
	mockTopicRepo.EXPECT().Save(gomock.Any(), dTopic)
	err := topicCore.CreateTopic(ctx, dTopic)
	assert.Nil(t, err)
}

func TestCore_List(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	topicCore := NewCore(mockTopicRepo, mockProjectCore, mockBrokerStore)
	ctx := context.Background()

	topics := []common.IModel{
		&Model{
			Name: "project_1/topic_1",
		},
		&Model{
			Name: "project_1/topic_2",
		},
	}

	prefix := "subscriptions/project_1"
	mockTopicRepo.EXPECT().List(ctx, common.GetBasePrefix()+prefix).Return(topics, nil)
	out, err := topicCore.List(ctx, prefix)

	expectedOutput := []*Model{
		{
			Name: "project_1/topic_1",
		},
		{
			Name: "project_1/topic_2",
		},
	}

	assert.Nil(t, err)
	assert.Equal(t, expectedOutput, out)
}

func TestCore_CreateSubscriptionTopic(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		brokerStore brokerstore.IBrokerStore
	}
	type args struct {
		ctx   context.Context
		model *Model
	}
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	mockAdmin := messagebrokermock.NewMockBroker(ctrl)
	ctx := context.Background()
	dTopic := getDummyTopicModel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Test1",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:   ctx,
				model: dTopic,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Core{
				repo:        tt.fields.repo,
				projectCore: tt.fields.projectCore,
				brokerStore: tt.fields.brokerStore,
			}
			mockBrokerStore.EXPECT().GetAdmin(gomock.Any(), messagebroker.AdminClientOptions{}).Return(mockAdmin, nil)
			mockAdmin.EXPECT().CreateTopic(gomock.Any(), messagebroker.CreateTopicRequest{dTopic.Name, DefaultNumPartitions}).Return(messagebroker.CreateTopicResponse{}, nil)
			if err := c.CreateSubscriptionTopic(tt.args.ctx, tt.args.model); (err != nil) != tt.wantErr {
				t.Errorf("Core.CreateSubscriptionTopic() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCore_ExistsWithName(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		brokerStore brokerstore.IBrokerStore
	}
	type args struct {
		ctx       context.Context
		name      string
		projectID string
		topicName string
	}
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	ctx := context.Background()
	dTopic := getDummyTopicModel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "Check if Topic exists with name.",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				name:      dTopic.Name,
				projectID: dTopic.ExtractedProjectID,
				topicName: dTopic.ExtractedTopicName,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Throw error for invalid topic name.",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				name:      "",
				projectID: "",
				topicName: "",
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Core{
				repo:        tt.fields.repo,
				projectCore: tt.fields.projectCore,
				brokerStore: tt.fields.brokerStore,
			}
			var err2 error = nil
			var expectBool bool = true
			if len(tt.args.name) == 0 {
				err2 = fmt.Errorf("Invalid Project Name!")
				expectBool = false
			} else {
				mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+Prefix+tt.args.projectID+"/"+tt.args.topicName).Return(expectBool, err2)
			}
			got, err := c.ExistsWithName(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Core.ExistsWithName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Core.ExistsWithName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCore_DeleteTopic(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		brokerStore brokerstore.IBrokerStore
	}
	type args struct {
		ctx       context.Context
		m         *Model
		name      string
		projectID string
		topicName string
	}
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	ctx := context.Background()
	dTopic := getDummyTopicModel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Delete Topic Successfully",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				m:         dTopic,
				name:      dTopic.Name,
				projectID: dTopic.ExtractedProjectID,
				topicName: dTopic.ExtractedTopicName,
			},
			wantErr: false,
		},
		{
			name: "Deleting Topic failure",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				m:         dTopic,
				name:      "",
				projectID: "",
				topicName: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Core{
				repo:        tt.fields.repo,
				projectCore: tt.fields.projectCore,
				brokerStore: tt.fields.brokerStore,
			}
			var err2 error = nil
			var expectBool bool = true
			if len(tt.args.name) == 0 {
				err2 = fmt.Errorf("Invalid Project Name!")
				expectBool = false
				mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), dTopic.ExtractedProjectID).Return(expectBool, err2)
			} else {
				mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), dTopic.ExtractedProjectID).Return(expectBool, err2)
				mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+Prefix+tt.args.projectID+"/"+tt.args.topicName).Return(expectBool, err2)
				mockTopicRepo.EXPECT().Delete(gomock.Any(), tt.args.m).Return(err2)
			}
			if err := c.DeleteTopic(tt.args.ctx, tt.args.m); (err != nil) != tt.wantErr {
				t.Errorf("Core.DeleteTopic() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCore_DeleteProjectTopics(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		brokerStore brokerstore.IBrokerStore
	}
	type args struct {
		ctx       context.Context
		projectID string
		topicName string
	}

	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	ctx := context.Background()
	dTopic := getDummyTopicModel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Delete Topic Tree Successfully",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				projectID: dTopic.ExtractedProjectID,
				topicName: dTopic.ExtractedTopicName,
			},
			wantErr: false,
		},
		{
			name: "Delete Topic Tree hits Error",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				projectID: "",
				topicName: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Core{
				repo:        tt.fields.repo,
				projectCore: tt.fields.projectCore,
				brokerStore: tt.fields.brokerStore,
			}
			var err2 error = nil
			if len(tt.args.projectID) == 0 {
				err2 = fmt.Errorf("Invalid Project ID!")
			} else {
				mockTopicRepo.EXPECT().DeleteTree(gomock.Any(), common.GetBasePrefix()+Prefix+tt.args.projectID).Return(err2)
			}
			if err := c.DeleteProjectTopics(tt.args.ctx, tt.args.projectID); (err != nil) != tt.wantErr {
				t.Errorf("Core.DeleteProjectTopics() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCore_UpdateTopic(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		brokerStore brokerstore.IBrokerStore
	}
	type args struct {
		ctx       context.Context
		m         *Model
		projectID string
	}
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	ctx := context.Background()
	dTopic := getDummyTopicModel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Update Topic successfully",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				m:         dTopic,
				projectID: dTopic.ExtractedProjectID,
			},
			wantErr: false,
		},
		{
			name: "Get error due to invalid Project ID",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				m:         dTopic,
				projectID: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Core{
				repo:        tt.fields.repo,
				projectCore: tt.fields.projectCore,
				brokerStore: tt.fields.brokerStore,
			}
			var err2 error = nil
			var expectBool = true
			if len(tt.args.projectID) == 0 {
				err2 = fmt.Errorf("Invalid Project ID!")
				expectBool = false
				mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+Prefix+tt.args.m.ExtractedProjectID+"/"+tt.args.m.ExtractedTopicName).Return(expectBool, err2)
			} else {
				mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+Prefix+tt.args.m.ExtractedProjectID+"/"+tt.args.m.ExtractedTopicName).Return(expectBool, err2)
				mockTopicRepo.EXPECT().Save(gomock.Any(), tt.args.m)
			}
			if err := c.UpdateTopic(tt.args.ctx, tt.args.m); (err != nil) != tt.wantErr {
				t.Errorf("Core.UpdateTopic() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCore_Get(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		brokerStore brokerstore.IBrokerStore
	}
	type args struct {
		ctx       context.Context
		key       string
		projectID string
		topicName string
	}
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	ctx := context.Background()
	dTopic := getDummyTopicModel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Model
		wantErr bool
	}{
		{
			name: "Successfully Get Topic",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				key:       dTopic.Name,
				projectID: dTopic.ExtractedProjectID,
				topicName: dTopic.ExtractedTopicName,
			},
			want:    dTopic,
			wantErr: false,
		},
		{
			name: "Error while Get Topic",
			fields: fields{
				repo:        mockTopicRepo,
				projectCore: mockProjectCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:       ctx,
				key:       "",
				projectID: "",
				topicName: "",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Core{
				repo:        tt.fields.repo,
				projectCore: tt.fields.projectCore,
				brokerStore: tt.fields.brokerStore,
			}
			var err2 error = nil
			if len(tt.args.projectID) == 0 {
				err2 = fmt.Errorf("Invalid Project ID!")
			} else {
				mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+Prefix+tt.args.projectID+"/"+tt.args.topicName).Return(true, nil)
				mockTopicRepo.EXPECT().Get(gomock.Any(), gomock.Any(), &Model{}).Do(func(arg1 context.Context, arg2 string, mod *Model) {
					if err2 == nil {
						mod.Name = "projects/test-project/topics/test-topic"
						mod.Labels = map[string]string{"label": "value"}
						mod.ExtractedProjectID = "test-project"
						mod.ExtractedTopicName = "test-topic"
						mod.NumPartitions = DefaultNumPartitions
					}
				}).Return(err2)
			}
			got, err := c.Get(tt.args.ctx, tt.args.key)
			fmt.Println(err)
			if (err != nil) != tt.wantErr {
				t.Errorf("Core.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Core.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}
