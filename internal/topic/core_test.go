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

	tests := []struct {
		topicModel *Model
	}{
		{
			topicModel: getDummyTopicModel(),
		},
		{
			topicModel: getDLQDummyTopicModel("test-topic-dlq"),
		},
	}

	for _, test := range tests {
		dTopic := test.topicModel
		mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), dTopic.ExtractedProjectID).Return(true, nil)
		mockBrokerStore.EXPECT().GetAdmin(gomock.Any(), messagebroker.AdminClientOptions{}).Return(mockAdmin, nil)
		mockAdmin.EXPECT().CreateTopic(gomock.Any(), messagebroker.CreateTopicRequest{Name: dTopic.Name, NumPartitions: DefaultNumPartitions, Config: dTopic.GetRetentionConfig()}).Return(messagebroker.CreateTopicResponse{}, nil)
		mockTopicRepo.EXPECT().Exists(gomock.Any(), dTopic.Key())
		mockTopicRepo.EXPECT().Save(gomock.Any(), dTopic)
		err := topicCore.CreateTopic(ctx, dTopic)
		assert.Nil(t, err)
	}
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
			name: "Create Subscription successfully",
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
	c := &Core{
		repo:        mockTopicRepo,
		projectCore: mockProjectCore,
		brokerStore: mockBrokerStore,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBrokerStore.EXPECT().GetAdmin(gomock.AssignableToTypeOf(ctx), messagebroker.AdminClientOptions{}).Return(mockAdmin, nil)
			mockAdmin.EXPECT().CreateTopic(gomock.AssignableToTypeOf(ctx), messagebroker.CreateTopicRequest{Name: dTopic.Name, NumPartitions: DefaultNumPartitions}).Return(messagebroker.CreateTopicResponse{}, nil)
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
	c := &Core{
		repo:        mockTopicRepo,
		projectCore: mockProjectCore,
		brokerStore: mockBrokerStore,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.args.name) != 0 {
				mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+Prefix+tt.args.projectID+"/"+tt.args.topicName).Return(true, nil)
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
	type args struct {
		m              *Model
		name           string
		cleanupEnabled bool
	}
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	mockAdmin := messagebrokermock.NewMockAdmin(ctrl)
	dTopic := getDummyTopicModel()
	retryTopic := getDummyTopicModel()
	retryTopic.Name += RetryTopicSuffix
	retryTopic.ExtractedTopicName += RetryTopicSuffix

	tests := []struct {
		name           string
		args           args
		brokerTopicErr error
		wantErr        bool
	}{
		{
			name: "Delete Topic Successfully",
			args: args{
				m:              dTopic,
				name:           dTopic.Name,
				cleanupEnabled: true,
			},
			wantErr: false,
		},
		{
			name: "Deleting Retry Topic successfully",
			args: args{
				m:              retryTopic,
				name:           retryTopic.Name,
				cleanupEnabled: false,
			},
			wantErr: false,
		},
		{
			name: "Deleting Topic failed due to broker topic deletion",
			args: args{
				m:              dTopic,
				name:           dTopic.Name,
				cleanupEnabled: true,
			},
			brokerTopicErr: fmt.Errorf("Broker Topic deletion error"),
			wantErr:        true,
		},
		{
			name: "Deleting Topic failure",
			args: args{
				m:              dTopic,
				name:           "",
				cleanupEnabled: true,
			},
			wantErr: true,
		},
	}
	c := &Core{
		repo:        mockTopicRepo,
		projectCore: mockProjectCore,
		brokerStore: mockBrokerStore,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err2 error = nil
			var expectBool bool = true
			if len(tt.args.name) == 0 {
				err2 = fmt.Errorf("Invalid Project Name!")
				expectBool = false
				mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), dTopic.ExtractedProjectID).Return(expectBool, err2)
			} else {
				mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), dTopic.ExtractedProjectID).Return(true, nil)
				mockTopicRepo.EXPECT().Exists(gomock.Any(), tt.args.m.Key()).Return(true, nil).AnyTimes()
				mockTopicRepo.EXPECT().Delete(gomock.Any(), tt.args.m).Return(nil).AnyTimes()
				mockBrokerStore.EXPECT().IsTopicCleanUpEnabled().Return(tt.args.cleanupEnabled)
				mockAdmin.EXPECT().DeleteTopic(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req messagebroker.DeleteTopicRequest) (*messagebroker.DeleteTopicResponse, error) {
						if tt.brokerTopicErr != nil {
							return nil, tt.brokerTopicErr
						}
						return &messagebroker.DeleteTopicResponse{}, nil
					}).MaxTimes(1)
				mockBrokerStore.EXPECT().GetAdmin(gomock.Any(), gomock.Any()).Return(mockAdmin, nil).MaxTimes(1)
			}
			if err := c.DeleteTopic(context.Background(), tt.args.m); (err != nil) != tt.wantErr {
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
	c := &Core{
		repo:        mockTopicRepo,
		projectCore: mockProjectCore,
		brokerStore: mockBrokerStore,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.args.projectID) != 0 {
				mockTopicRepo.EXPECT().DeleteTree(gomock.AssignableToTypeOf(ctx), common.GetBasePrefix()+Prefix+tt.args.projectID).Return(nil)
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
	c := &Core{
		repo:        mockTopicRepo,
		projectCore: mockProjectCore,
		brokerStore: mockBrokerStore,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err2 error = nil
			var expectBool = true
			if len(tt.args.projectID) == 0 {
				err2 = fmt.Errorf("Invalid Project ID!")
				expectBool = false
				mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+Prefix+tt.args.m.ExtractedProjectID+"/"+tt.args.m.ExtractedTopicName).Return(expectBool, err2)
			} else {
				mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+Prefix+tt.args.m.ExtractedProjectID+"/"+tt.args.m.ExtractedTopicName).Return(true, nil)
				mockTopicRepo.EXPECT().Save(gomock.AssignableToTypeOf(ctx), tt.args.m)
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
	c := &Core{
		repo:        mockTopicRepo,
		projectCore: mockProjectCore,
		brokerStore: mockBrokerStore,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

func TestCore_SetupTopicRetentionConfigs(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	dlqTopic := getDLQDummyTopicModel("test-topic-dlq")
	primaryTopic := getDummyTopicModel()
	dlqTopic2 := getDLQDummyTopicModel("test-topic-2-dlq")

	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockAdmin := messagebrokermock.NewMockBroker(ctrl)

	tests := []struct {
		name            string
		topicsToUpdate  map[string]*Model
		topics          []common.IModel
		existingConfigs map[string]map[string]string
		expected        []string
		listErr         error
		getAdminErr     error
		expectedErr     error
	}{
		{
			name:            "Alter retention configs with non dlq topics, without error",
			topics:          []common.IModel{primaryTopic, dlqTopic, dlqTopic2},
			existingConfigs: map[string]map[string]string{dlqTopic2.Name: dlqTopic2.GetRetentionConfig()},
			expected:        []string{dlqTopic.Name},
		},
		{
			name:           "Alter retention configs with error in altering configs",
			topicsToUpdate: map[string]*Model{dlqTopic.Name: dlqTopic},
			topics:         []common.IModel{dlqTopic},
			expectedErr:    fmt.Errorf("Something went wrong"),
		},
		{
			name:        "Alter retention configs with error in getting admin server",
			topics:      []common.IModel{dlqTopic},
			getAdminErr: fmt.Errorf("Something went wrong"),
			expectedErr: fmt.Errorf("Something went wrong"),
		},
		{
			name: "Alter retention configs without any existing topic",
		},
		{
			name:        "Alter retention configs with error in listing topics",
			listErr:     fmt.Errorf("Something went wrong"),
			expectedErr: fmt.Errorf("Something went wrong"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			topicNames := make([]string, 0, len(test.topicsToUpdate))
			if len(test.topicsToUpdate) > 0 {
				for name, model := range test.topicsToUpdate {
					topicNames = append(topicNames, name)
					mockTopicRepo.EXPECT().Exists(gomock.Any(), model.Key()).Return(true, nil).MaxTimes(1)
					mockTopicRepo.EXPECT().Get(gomock.Any(), model.Key(), gomock.Any()).DoAndReturn(func(arg1 context.Context, name string, mod *Model) error {
						mod.Name = model.Name
						mod.ExtractedTopicName = model.ExtractedTopicName
						return nil
					}).MaxTimes(1)
				}
			} else {
				mockTopicRepo.EXPECT().List(ctx, common.GetBasePrefix()+Prefix).Return(test.topics, test.listErr).MaxTimes(1)
			}

			mockAdmin.EXPECT().AlterTopicConfigs(ctx, gomock.Any()).Return(test.expected, test.expectedErr).MaxTimes(1)
			mockAdmin.EXPECT().DescribeTopicConfigs(ctx, gomock.Any()).Return(test.existingConfigs, nil).MaxTimes(1)
			mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
			mockBrokerStore.EXPECT().GetAdmin(gomock.Any(), messagebroker.AdminClientOptions{}).Return(mockAdmin, test.getAdminErr).MaxTimes(1)
			c := &Core{
				repo:        mockTopicRepo,
				projectCore: projectcoremock.NewMockICore(ctrl),
				brokerStore: mockBrokerStore,
			}
			got, err := c.SetupTopicRetentionConfigs(ctx, topicNames)
			assert.Equal(t, test.expectedErr != nil, err != nil)
			assert.Equal(t, test.expected, got)
		})
	}
}

func getTopicModelWithInput(name, extractedTopicName, extractedProjectID string) *Model {
	return &Model{
		Name:               name,
		ExtractedTopicName: extractedTopicName,
		ExtractedProjectID: extractedProjectID,
	}
}
