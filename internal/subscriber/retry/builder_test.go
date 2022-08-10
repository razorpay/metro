package retry

import (
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/cache"
	mocks2 "github.com/razorpay/metro/pkg/cache/mocks"
	"github.com/stretchr/testify/assert"
)

func TestRetrier_Build(t *testing.T) {
	type fields struct {
		subscriberID   string
		subs           *subscription.Model
		bs             brokerstore.IBrokerStore
		ch             cache.ICache
		backoff        Backoff
		finder         IntervalFinder
		handler        MessageHandler
		delayConsumers sync.Map
		errChan        chan error
	}
	ctrl := gomock.NewController(t)
	bs := mocks.NewMockIBrokerStore(ctrl)
	ch := mocks2.NewMockICache(ctrl)
	subs := getMockSubscriptionModel()
	errCh := make(chan error)
	tests := []struct {
		name   string
		fields fields
		want   IRetrier
	}{
		{
			name: "Test Retrier Builder",
			fields: fields{
				subscriberID:   "subscription-id",
				subs:           subs,
				bs:             bs,
				ch:             ch,
				backoff:        NewFixedWindowBackoff(),
				finder:         NewClosestIntervalWithCeil(),
				handler:        NewPushToPrimaryRetryTopicHandler(bs),
				delayConsumers: sync.Map{},
				errChan:        errCh,
			},
			want: &Retrier{
				subscriberID:   "subscription-id",
				subs:           subs,
				bs:             bs,
				ch:             ch,
				backoff:        NewFixedWindowBackoff(),
				finder:         NewClosestIntervalWithCeil(),
				handler:        NewPushToPrimaryRetryTopicHandler(bs),
				delayConsumers: sync.Map{},
				errChan:        errCh,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retierBuilder := NewRetrierBuilder().
				WithSubscriberID(tt.fields.subscriberID).
				WithBrokerStore(tt.fields.bs).
				WithSubscription(tt.fields.subs).
				WithBackoff(tt.fields.backoff).
				WithCache(tt.fields.ch).
				WithErrChan(tt.fields.errChan).
				WithIntervalFinder(tt.fields.finder).
				WithMessageHandler(tt.fields.handler)
			assert.Equalf(t, tt.want, retierBuilder.Build(), "Build()")
		})
	}
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
			MinimumBackoff: 10,
			MaximumBackoff: 30,
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
