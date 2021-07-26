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
		PushEndpoint:                   "https://www.razorpay.com/api/v1",
		AckDeadlineSec:                 20,
	}

	existing := Model{
		Name:                           "projects/project123/subscriptions/subscription123",
		Topic:                          "projects/project123/subscriptions/subscription123",
		ExtractedSubscriptionProjectID: "project123",
		PushEndpoint:                   "https://www.razorpay.com/api",
	}

	expected := Model{
		Name:                           "projects/project123/subscriptions/subscription123",
		Topic:                          "projects/project123/subscriptions/subscription123",
		ExtractedSubscriptionProjectID: "project123",
		PushEndpoint:                   "https://www.razorpay.com/api/v1",
	}

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(true, nil)
	mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), &Model{}).Times(1).Do(func(ctx context.Context, key string, model *Model) error {
		*model = existing
		return nil
	})
	mockRepo.EXPECT().Save(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	res, err := core.UpdateSubscription(ctx, &sub, []string{"PushEndpoint", "Credentials"})
	assert.Nil(t, err)
	assert.Equal(t, &expected, res, "Wrong updation of subscription")
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
		PushEndpoint:                   "https://www.razorpay.com/api/v1",
		AckDeadlineSec:                 20,
	}

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(false, nil)

	_, err := core.UpdateSubscription(ctx, &sub, []string{"PushEndpoint", "Credentials"})
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
		PushEndpoint:                   "https://www.razorpay.com/api/v1",
		AckDeadlineSec:                 20,
	}

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(false, nil)

	_, err := core.UpdateSubscription(ctx, &sub, []string{"PushEndpoint", "Credentials"})
	assert.NotNil(t, err)
}
