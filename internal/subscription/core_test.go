// +build unit

package subscription

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	pCore "github.com/razorpay/metro/internal/project/mocks/core"
	repo "github.com/razorpay/metro/internal/subscription/mocks/repo"
	tCore "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/stretchr/testify/assert"
)

func TestSubscriptionCore_UpdateSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)
	sub := Model{
		Name:                           "projects/project123/subscriptions/subscription123",
		Topic:                          "projects/project123/subscriptions/subscription123",
		ExtractedSubscriptionProjectID: "project123",
		PushConfig: &PushConfig{
			PushEndpoint: "https://www.razorpay.com/api/v1",
		},
		AckDeadlineSeconds: 20,
	}

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
	sub := Model{
		Name:                           "projects/project123/subscriptions/subscription123",
		Topic:                          "projects/project123/subscriptions/subscription123",
		ExtractedSubscriptionProjectID: "project123",
		PushConfig: &PushConfig{
			PushEndpoint: "https://www.razorpay.com/api/v1",
		},
		AckDeadlineSeconds: 20,
	}

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
	sub := Model{
		Name:                           "projects/project123/subscriptions/subscription123",
		Topic:                          "projects/project123/subscriptions/subscription123",
		ExtractedSubscriptionProjectID: "project123",
		PushConfig: &PushConfig{
			PushEndpoint: "https://www.razorpay.com/api/v1",
		},
		AckDeadlineSeconds: 20,
	}

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(false, nil)

	err := core.UpdateSubscription(ctx, &sub)
	assert.NotNil(t, err)
}
