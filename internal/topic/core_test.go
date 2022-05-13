//go:build unit
// +build unit

package topic

import (
	"context"
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

// func TestCore_ExistsWithName(t *testing.T) {
// 	type fields struct {
// 		repo        IRepo
// 		projectCore project.ICore
// 		brokerStore brokerstore.IBrokerStore
// 	}
// 	type args struct {
// 		ctx  context.Context
// 		name string
// 	}
// 	ctrl := gomock.NewController(t)
// 	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
// 	mockProjectCore := projectcoremock.NewMockICore(ctrl)
// 	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
// 	// mockAdmin := messagebrokermock.NewMockBroker(ctrl)
// 	ctx := context.Background()
// 	dTopic := getDummyTopicModel()

// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		args    args
// 		want    bool
// 		wantErr bool
// 	}{
// 		{
// 			name: "Test1",
// 			fields: fields{
// 				repo:        mockTopicRepo,
// 				projectCore: mockProjectCore,
// 				brokerStore: mockBrokerStore,
// 			},
// 			args: args{
// 				ctx:  ctx,
// 				name: dTopic.Name,
// 			},
// 			want:    true,
// 			wantErr: false,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			c := &Core{
// 				repo:        tt.fields.repo,
// 				projectCore: tt.fields.projectCore,
// 				brokerStore: tt.fields.brokerStore,
// 			}
// 			// mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+tt.args.name)
// 			got, err := c.ExistsWithName(tt.args.ctx, tt.args.name)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("Core.ExistsWithName() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if got != tt.want {
// 				t.Errorf("Core.ExistsWithName() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
