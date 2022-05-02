// +build unit

package subscription

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	pCore "github.com/razorpay/metro/internal/project/mocks/core"
	repo "github.com/razorpay/metro/internal/subscription/mocks/repo"
	"github.com/razorpay/metro/internal/topic"
	tCore "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/stretchr/testify/assert"
)

func TestSubscriptionCore_CreateSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)

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

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)
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

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)
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

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)
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

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)
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
