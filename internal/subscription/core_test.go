// +build unit

package subscription

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/project"
	pCore "github.com/razorpay/metro/internal/project/mocks/core"
	repo "github.com/razorpay/metro/internal/subscription/mocks/repo"
	"github.com/razorpay/metro/internal/topic"
	tCore "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/messagebroker"
	messagebrokermock "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"
)

func TestSubscriptionCore_CreateSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)

	sub := getSubModel()

	tpc := topic.Model{
		Name:               sub.GetSubscriptionTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedTopicProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(false, nil)
	mockTopicCore.EXPECT().Get(gomock.Any(), sub.Topic).Times(1).Return(&tpc, nil)
	mockTopicCore.EXPECT().CreateSubscriptionTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetSubscriptionTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateRetryTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetRetryTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName + topic.RetryTopicSuffix,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateDeadLetterTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetDeadLetterTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName + topic.DeadLetterTopicSuffix,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateTopic(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).Times(8) // number of delay topics being created
	mockRepo.EXPECT().Save(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	err := core.CreateSubscription(ctx, &sub)
	assert.Nil(t, err)
}

func TestSubscriptionCore_CreateSubscription_DLQTopic(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)
	sub := getSubModel()
	sub.Topic = "projects/project123/topics/subscription123-dlq"
	sub.ExtractedTopicName = "subscription123-dlq"
	tpc := topic.Model{
		Name:               sub.Topic,
		ExtractedTopicName: sub.ExtractedTopicName,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedTopicProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(false, nil)
	mockTopicCore.EXPECT().Get(gomock.Any(), sub.Topic).Times(1).Return(&tpc, nil)
	mockTopicCore.EXPECT().CreateSubscriptionTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetSubscriptionTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateRetryTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetRetryTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName + topic.RetryTopicSuffix,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateDeadLetterTopic(gomock.Any(), gomock.Any()).Times(0).Return(nil)
	mockTopicCore.EXPECT().CreateTopic(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).Times(8) // number of delay topics being created
	mockRepo.EXPECT().Save(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	err := core.CreateSubscription(ctx, &sub)
	assert.Nil(t, err)
}

func TestSubscriptionCore_UpdateSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)
	sub := getSubModel()

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(true, nil)
	mockRepo.EXPECT().Save(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	err := core.UpdateSubscription(ctx, &sub)
	assert.Nil(t, err)
}

func TestSubscriptionCore_UpdateSubscriptionNotExists(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)
	sub := getSubModel()

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(false, nil)

	err := core.UpdateSubscription(ctx, &sub)
	assert.NotNil(t, err)
}

func TestSubscriptionCore_UpdateSubscriptionProjectNotExists(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)
	sub := getSubModel()

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(false, nil)

	err := core.UpdateSubscription(ctx, &sub)
	assert.NotNil(t, err)
}

func TestSubscriptionCore_CreateDelayTopics(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockTopicCore := tCore.NewMockICore(ctrl)

	topic := &topic.Model{
		Name:          "topic",
		NumPartitions: 2,
	}

	err := createDelayTopics(ctx, nil, mockTopicCore, topic)
	assert.Nil(t, err)

	err = createDelayTopics(ctx, &Model{}, nil, topic)
	assert.Nil(t, err)

	err = createDelayTopics(ctx, nil, nil, topic)
	assert.Nil(t, err)

	sub := getSubModel()

	mockTopicCore.EXPECT().CreateTopic(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).Times(8) // number of delay topics being created
	err = createDelayTopics(ctx, &sub, mockTopicCore, topic)
	assert.Nil(t, err)
}

func getSubModel() Model {
	sub := Model{
		Name:                           "projects/project123/subscriptions/subscription123",
		Topic:                          "projects/project123/topics/topic123",
		ExtractedSubscriptionProjectID: "project123",
		ExtractedTopicProjectID:        "project123",
		ExtractedSubscriptionName:      "subscription123",
		ExtractedTopicName:             "topic123",
		PushConfig: &PushConfig{
			PushEndpoint: "https://www.razorpay.com/api/v1",
		},
		AckDeadlineSeconds: 20,
		Labels:             map[string]string{},
	}
	return sub
}

func TestCore_RescaleSubTopics(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		topicCore   topic.ICore
		brokerStore brokerstore.IBrokerStore
	}
	type args struct {
		ctx        context.Context
		topicModel *topic.Model
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockTopicCore.EXPECT().UpdateTopic(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockRepo := repo.NewMockIRepo(ctrl)
	mockAdmin := messagebrokermock.NewMockBroker(ctrl)
	mockAdmin.EXPECT().AddTopicPartitions(gomock.Any(), gomock.Any()).Return(&messagebroker.AddTopicPartitionResponse{}, nil).AnyTimes()

	sub := getSubModel()
	expectedList := []common.IModel{
		&sub,
	}
	topic := &topic.Model{
		Name:               sub.GetTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      1,
	}
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	mockBrokerStore.EXPECT().GetAdmin(gomock.Any(), gomock.Any()).Return(mockAdmin, nil).AnyTimes()
	mockProjectCore.EXPECT().ListKeys(gomock.Any()).Return([]string{"metro/projects/project123"}, nil)
	mockRepo.EXPECT().List(gomock.Any(), gomock.Any()).Return(expectedList, nil)

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Rescale without errors",
			fields: fields{
				repo:        mockRepo,
				projectCore: mockProjectCore,
				topicCore:   mockTopicCore,
				brokerStore: mockBrokerStore,
			},
			args: args{
				ctx:        ctx,
				topicModel: topic,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Core{
				repo:        tt.fields.repo,
				projectCore: tt.fields.projectCore,
				topicCore:   tt.fields.topicCore,
				brokerStore: tt.fields.brokerStore,
			}
			if err := c.RescaleSubTopics(tt.args.ctx, tt.args.topicModel); (err != nil) != tt.wantErr {
				t.Errorf("Core.RescaleSubTopics() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCore_DeleteSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)
	sub := getSubModel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "Delete subscription with error",
			wantErr: true,
		},
		{
			name:    "Delete subscription without error",
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(!test.wantErr, nil)
			mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, prefix string, model *Model) error {
					model.Topic = sub.Topic
					model.ExtractedTopicName = sub.ExtractedTopicName
					return nil
				}).MaxTimes(1)
			mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).MaxTimes(1).Return(true, nil)
			mockRepo.EXPECT().Delete(gomock.Any(), gomock.Any()).MaxTimes(1).Return(nil)
			mockTopicCore.EXPECT().DeleteTopic(ctx, gomock.Any()).Return(nil).AnyTimes()
			err := core.DeleteSubscription(ctx, &sub)
			assert.Equal(t, test.wantErr, err != nil)
		})
	}
}
func TestCore_DeleteSubscriptionTopics(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionName := "projects/test-project/subscriptions/test-sub"
	projectID := "test-project"
	subTopic := "projects/test-project/topics/test-topic"
	subName := "test-sub"
	topicName := "test-topic"
	subscription := getSubModelWithInput(subscriptionName, subTopic, projectID, projectID, subName, topicName)
	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)
	subTopicsMap := getSubTopicsMap(subscription)
	type args struct {
		ctx           context.Context
		subscription  *Model
		subsTopicsMap map[string]bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "DeleteSubscriptionTopic sucesss",
			args: args{
				ctx:           ctx,
				subscription:  subscription,
				subsTopicsMap: subTopicsMap,
			},
			wantErr: false,
		},
		{
			name: "DeleteSubscriptionTopic failure subscription with empty topic",
			args: args{
				ctx:           ctx,
				subscription:  getSubModelWithInput(subscriptionName, "", projectID, projectID, subName, topicName),
				subsTopicsMap: subTopicsMap,
			},
			wantErr: true,
		},
		{
			name: "DeleteSubscriptionTopic failure error from topic core",
			args: args{
				ctx:           ctx,
				subscription:  subscription,
				subsTopicsMap: subTopicsMap,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), tt.args.subscription.ExtractedSubscriptionProjectID).DoAndReturn(func(arg0 context.Context, arg1 string) (bool, error) {
				if arg1 == tt.args.subscription.ExtractedSubscriptionProjectID {
					return true, nil
				}
				return false, errors.New("project not found")
			}).MaxTimes(1)
			mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(arg0 context.Context, arg1 string, model *Model) error {
				model.Topic = tt.args.subscription.Topic
				model.ExtractedTopicName = tt.args.subscription.ExtractedTopicName
				return nil
			}).MaxTimes(1)
			mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(true, nil).MaxTimes(1)
			mockRepo.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil).MaxTimes(1)
			mockTopicCore.EXPECT().DeleteTopic(ctx, gomock.Any()).DoAndReturn(func(arg0 context.Context, arg1 *topic.Model) error {
				if tt.wantErr {
					return errors.New("test error")
				}
				return nil
			}).AnyTimes()
			err := core.DeleteSubscription(ctx, tt.args.subscription)
			assert.Equal(t, err != nil, tt.wantErr)
		})
	}
}

func TestCore_DeleteProjectSubscriptions(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)

	mockRepo.EXPECT().DeleteTree(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	err := core.DeleteProjectSubscriptions(ctx, "project123")
	assert.Nil(t, err)
}

func TestCore_GetTopicFromSubscriptionName(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	sub := getSubModel()

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)
	mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, prefix string, model *Model) error {
			model.Topic = sub.Topic
			return nil
		})

	topic, err := core.GetTopicFromSubscriptionName(ctx, sub.Name)
	assert.Nil(t, err)
	assert.Equal(t, sub.Topic, topic)
}

func TestCore_ListKeys(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore, mockBrokerStore)
	mockRepo.EXPECT().ListKeys(ctx, common.GetBasePrefix()+Prefix).Return([]string{"sub01"}, nil)
	resp, err := core.ListKeys(ctx, Prefix)
	assert.NoError(t, err)
	assert.Equal(t, []string{"sub01"}, resp)
}

func getSubModelWithInput(name, topic, subProjectID, topicProjectID, subName, topicName string) *Model {
	return &Model{
		Name:                           name,
		Topic:                          topic,
		ExtractedSubscriptionProjectID: subProjectID,
		ExtractedTopicProjectID:        topicProjectID,
		ExtractedSubscriptionName:      subName,
		ExtractedTopicName:             topicName,
		PushConfig: &PushConfig{
			PushEndpoint: "https://www.razorpay.com/api/v1",
		},
		AckDeadlineSeconds: 20,
		Labels:             map[string]string{},
	}
}

func getSubTopicsMap(subs *Model) map[string]bool {
	subTopicsMap := map[string]bool{
		subs.GetSubscriptionTopic(): true,
		subs.GetRetryTopic():        true,
	}
	for _, delayTopic := range subs.GetDelayTopics() {
		subTopicsMap[delayTopic] = true
	}
	if !subs.IsDeadLetterSubscription() {
		subTopicsMap[subs.GetDeadLetterTopic()] = true
	}
	return subTopicsMap
}
