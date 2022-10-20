package retry

import (
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/subscription"
	mocks2 "github.com/razorpay/metro/pkg/cache/mocks"
	"github.com/stretchr/testify/assert"
)

func TestRetrier_Build(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mocks.NewMockIBrokerStore(ctrl)
	ch := mocks2.NewMockICache(ctrl)
	subs := getMockSubscriptionModel()
	errCh := make(chan error)

	expectedRetrier := &Retrier{
		subscriberID:   "subscription-id",
		subs:           subs,
		bs:             bs,
		ch:             ch,
		backoff:        subscription.NewFixedWindowBackoff(),
		finder:         subscription.NewClosestIntervalWithCeil(),
		handler:        NewPushToPrimaryRetryTopicHandler(bs),
		delayConsumers: sync.Map{},
		errChan:        errCh,
	}

	retrierBuilder := NewRetrierBuilder().
		WithSubscriberID("subscription-id").
		WithBrokerStore(bs).
		WithSubscription(subs).
		WithBackoff(subscription.NewFixedWindowBackoff()).
		WithCache(ch).
		WithErrChan(errCh).
		WithIntervalFinder(subscription.NewClosestIntervalWithCeil()).
		WithMessageHandler(NewPushToPrimaryRetryTopicHandler(bs))
	assert.Equal(t, expectedRetrier, retrierBuilder.Build())

}

func getMockSubscriptionModel() *subscription.Model {
	return &subscription.Model{
		Name:                           "projects/test-project/subscriptions/test-subscription",
		Labels:                         map[string]string{"label": "value"},
		ExtractedSubscriptionProjectID: "test-project",
		ExtractedTopicProjectID:        "test-project",
		ExtractedTopicName:             "test-topic",
		ExtractedSubscriptionName:      "test-subscription",
		Topic:                          "projects/test-project/topics/test-topic",
		DeadLetterPolicy: &subscription.DeadLetterPolicy{
			DeadLetterTopic:     "projects/test-project/topics/test-subscription-dlq",
			MaxDeliveryAttempts: 10,
		},
		RetryPolicy: &subscription.RetryPolicy{
			MinimumBackoff: 5,
			MaximumBackoff: 3600,
		},
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "www.some-url.com",
			Credentials: &credentials.Model{
				Username:  "u",
				Password:  "p",
				ProjectID: "dummy1",
			},
		},
	}
}
