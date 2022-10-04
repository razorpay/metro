//go:build unit
// +build unit

package web

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/pkg/errors"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/credentials"
	mocks5 "github.com/razorpay/metro/internal/credentials/mocks/core"
	"github.com/razorpay/metro/internal/project"
	mocks4 "github.com/razorpay/metro/internal/project/mocks/core"
	"github.com/razorpay/metro/internal/subscription"
	mocks2 "github.com/razorpay/metro/internal/subscription/mocks/core"
	cache2 "github.com/razorpay/metro/pkg/cache"
	cachemock "github.com/razorpay/metro/pkg/cache/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/razorpay/metro/service/web/stream"
	mocks3 "github.com/razorpay/metro/service/web/stream/mocks/manager"
)

var validAckId = "MS4yLjMuNA==_dGVzdC1zdWI=_dGVzdC10b3BpYw==_MA==_MA==_MTAw_dGVzdC1tZXNzYWdlLWlk"

func TestSubscriberServer_UpdateSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name:               "projects/project123/subscriptions/testsub",
			Topic:              "projects/project123/topics/test-topic",
			AckDeadlineSeconds: 30,
			Filter:             "abcd",
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"push_config"},
		},
	}

	current := &subscription.Model{
		Name:  req.Subscription.Name,
		Topic: req.Subscription.Topic,
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
		},
		AckDeadlineSeconds: 10,
	}

	expected := &metrov1.Subscription{
		Name:               "projects/project123/subscriptions/testsub",
		Topic:              "projects/project123/topics/test-topic",
		AckDeadlineSeconds: 10,
	}

	subscriptionCore.EXPECT().Get(gomock.Any(), req.Subscription.Name).Times(1).Return(current, nil)
	subscriptionCore.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	updated, err := server.UpdateSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, expected.Name, updated.Name)
	assert.Equal(t, expected.Topic, updated.Topic)
	assert.Equal(t, expected.PushConfig, updated.PushConfig)
	assert.Equal(t, expected.AckDeadlineSeconds, updated.AckDeadlineSeconds)
	assert.Equal(t, expected.Filter, updated.Filter)
}

func TestSubscriberServer_UpdateSubscriptionTestEmptyInRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name:               "projects/project123/subscriptions/testsub",
			Topic:              "projects/project123/topics/test-topic",
			AckDeadlineSeconds: 0,
			Filter:             "abcd",
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"push_config", "ack_deadline_seconds"},
		},
	}

	current := &subscription.Model{
		Name:  req.Subscription.Name,
		Topic: req.Subscription.Topic,
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
		},
		AckDeadlineSeconds: 30,
	}

	expected := &metrov1.Subscription{
		Name:               "projects/project123/subscriptions/testsub",
		Topic:              "projects/project123/topics/test-topic",
		AckDeadlineSeconds: 10,
	}

	subscriptionCore.EXPECT().Get(gomock.Any(), req.Subscription.Name).Times(1).Return(current, nil)
	subscriptionCore.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	updated, err := server.UpdateSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, expected.Name, updated.Name)
	assert.Equal(t, expected.Topic, updated.Topic)
	assert.Equal(t, expected.PushConfig, updated.PushConfig)
	assert.Equal(t, expected.AckDeadlineSeconds, updated.AckDeadlineSeconds)
	assert.Equal(t, expected.Filter, updated.Filter)
}

func TestSubscriberServer_UpdateSubscriptionTestEmptyInCurrent(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name:               "projects/project123/subscriptions/testsub",
			Topic:              "projects/project123/topics/test-topic",
			AckDeadlineSeconds: 30,
			Filter:             "abcd",
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"push_config", "ack_deadline_seconds"},
		},
	}

	current := &subscription.Model{
		Name:  req.Subscription.Name,
		Topic: req.Subscription.Topic,
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
		},
		AckDeadlineSeconds: 10,
	}

	expected := &metrov1.Subscription{
		Name:               "projects/project123/subscriptions/testsub",
		Topic:              "projects/project123/topics/test-topic",
		AckDeadlineSeconds: 30,
	}

	subscriptionCore.EXPECT().Get(gomock.Any(), req.Subscription.Name).Times(1).Return(current, nil)
	subscriptionCore.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	updated, err := server.UpdateSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, expected.Name, updated.Name)
	assert.Equal(t, expected.Topic, updated.Topic)
	assert.Equal(t, expected.PushConfig, updated.PushConfig)
	assert.Equal(t, expected.AckDeadlineSeconds, updated.AckDeadlineSeconds)
	assert.Equal(t, expected.Filter, updated.Filter)
}

func TestSubscriberServer_UpdateSubscriptionTestValidationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name:               "projects/project123/subscriptions/testsub",
			Topic:              "projects/project123/topics/test-topic",
			AckDeadlineSeconds: 30,
			Filter:             "abcd",
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"push_configa", "ack_deadline_seconds"},
		},
	}

	_, err := server.UpdateSubscription(ctx, req)
	assert.NotNil(t, err)
}

func TestSubscriberServer_UpdateSubscriptionRetryConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name:               "projects/project123/subscriptions/testsub",
			Topic:              "projects/project123/topics/test-topic",
			AckDeadlineSeconds: 30,
			RetryPolicy: &metrov1.RetryPolicy{
				MinimumBackoff: &durationpb.Duration{Seconds: 30},
				MaximumBackoff: &durationpb.Duration{Seconds: 300},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"retry_policy"},
		},
	}

	current := &subscription.Model{
		Name:  req.Subscription.Name,
		Topic: req.Subscription.Topic,
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
		},
		AckDeadlineSeconds: 10,
	}

	expected := &metrov1.Subscription{
		Name:  "projects/project123/subscriptions/testsub",
		Topic: "projects/project123/topics/test-topic",
		PushConfig: &metrov1.PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
		},
		AckDeadlineSeconds: 10,
		RetryPolicy: &metrov1.RetryPolicy{
			MinimumBackoff: &durationpb.Duration{Seconds: 30},
			MaximumBackoff: &durationpb.Duration{Seconds: 300},
		},
	}

	subscriptionCore.EXPECT().Get(gomock.Any(), req.Subscription.Name).Times(1).Return(current, nil)
	subscriptionCore.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	updated, err := server.UpdateSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, expected.Name, updated.Name)
	assert.Equal(t, expected.Topic, updated.Topic)
	assert.Equal(t, expected.PushConfig.PushEndpoint, updated.PushConfig.PushEndpoint)
	assert.Equal(t, expected.AckDeadlineSeconds, updated.AckDeadlineSeconds)
	assert.Equal(t, expected.RetryPolicy.MinimumBackoff.String(), updated.RetryPolicy.MinimumBackoff.String())
	assert.Equal(t, expected.RetryPolicy.MaximumBackoff.String(), updated.RetryPolicy.MaximumBackoff.String())
}

func TestSubscriberServer_UpdateSubscriptionDeadletterPolicy(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name:               "projects/project123/subscriptions/testsub",
			Topic:              "projects/project123/topics/test-topic",
			AckDeadlineSeconds: 30,
			DeadLetterPolicy: &metrov1.DeadLetterPolicy{
				MaxDeliveryAttempts: 10,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"dead_letter_policy"},
		},
	}

	current := &subscription.Model{
		Name:  req.Subscription.Name,
		Topic: req.Subscription.Topic,
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
		},
		AckDeadlineSeconds: 10,
	}

	expected := &metrov1.Subscription{
		Name:  "projects/project123/subscriptions/testsub",
		Topic: "projects/project123/topics/test-topic",
		PushConfig: &metrov1.PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
		},
		AckDeadlineSeconds: 10,
		DeadLetterPolicy: &metrov1.DeadLetterPolicy{
			MaxDeliveryAttempts: 10,
			DeadLetterTopic:     "projects/project123/topics/testsub-dlq",
		},
	}

	subscriptionCore.EXPECT().Get(gomock.Any(), req.Subscription.Name).Times(1).Return(current, nil)
	subscriptionCore.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	updated, err := server.UpdateSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, expected.Name, updated.Name)
	assert.Equal(t, expected.Topic, updated.Topic)
	assert.Equal(t, expected.PushConfig.PushEndpoint, updated.PushConfig.PushEndpoint)
	assert.Equal(t, expected.AckDeadlineSeconds, updated.AckDeadlineSeconds)
	assert.Equal(t, expected.DeadLetterPolicy.MaxDeliveryAttempts, updated.DeadLetterPolicy.MaxDeliveryAttempts)
	assert.Equal(t, expected.DeadLetterPolicy.DeadLetterTopic, updated.DeadLetterPolicy.DeadLetterTopic)
}

func TestSubscriberServer_CreateSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.Subscription{
		Name:  "projects/project321/subscriptions/testsub",
		Topic: "projects/project123/topics/test-topic",
	}

	m, err := subscription.GetValidatedModelForCreate(ctx, req)
	assert.Nil(t, err)

	subscriptionCore.EXPECT().CreateSubscription(gomock.Any(), m).Times(1).Return(nil)
	res, err := server.CreateSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, res, req)
}

func TestSubscriberServer_CreateSubscriptionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.Subscription{
		Name:  "projects/project321/subscriptions/testsub",
		Topic: "projects/project123/topics/test-topic",
	}

	m, err := subscription.GetValidatedModelForCreate(ctx, req)
	assert.Nil(t, err)

	subscriptionCore.EXPECT().CreateSubscription(gomock.Any(), m).Times(1).Return(fmt.Errorf("error"))
	res, err := server.CreateSubscription(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestSubscriberServer_Acknowledge(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.AcknowledgeRequest{
		Subscription: "projects/project321/subscriptions/testsub",
		AckIds:       []string{validAckId},
	}
	parsedReq, parseErr := stream.NewParsedAcknowledgeRequest(req)
	assert.Nil(t, parseErr)

	manager.EXPECT().Acknowledge(gomock.Any(), parsedReq).Times(1).Return(nil)

	res, err := server.Acknowledge(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, new(emptypb.Empty), res)
}

func TestSubscriberServer_AcknowledgeFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.AcknowledgeRequest{
		Subscription: "projects/project321/subscriptions/testsub",
		AckIds:       []string{validAckId},
	}
	parsedReq, parseErr := stream.NewParsedAcknowledgeRequest(req)
	assert.Nil(t, parseErr)

	manager.EXPECT().Acknowledge(gomock.Any(), parsedReq).Times(1).Return(fmt.Errorf("error"))

	res, err := server.Acknowledge(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestSubscriberServer_Pull(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.PullRequest{
		Subscription: "projects/project321/subscriptions/testsub",
		MaxMessages:  10,
	}

	res, err := server.Pull(ctx, req)
	assert.NotNil(t, res)
	assert.Nil(t, err)

}

func TestSubscriberServer_DeleteSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.DeleteSubscriptionRequest{
		Subscription: "projects/project123/subscriptions/sub-test",
	}
	subModel := getSubModel(req.Subscription, "projects/project123/topics/test-topic", "project123", "project123", "sub-test")

	subscriptionCore.EXPECT().Get(gomock.Any(), req.Subscription).Return(subModel, nil)
	subscriptionCore.EXPECT().DeleteSubscription(gomock.Any(), subModel).Times(1).Return(nil)
	res, err := server.DeleteSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, &emptypb.Empty{}, res)
}

func TestSubscriberServer_DeleteSubscription_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	ctx := context.Background()
	req := &metrov1.DeleteSubscriptionRequest{
		Subscription: "projects/project123/subscriptions/sub-test",
	}
	invalidReq := &metrov1.DeleteSubscriptionRequest{
		Subscription: "invalid-sub",
	}
	type args struct {
		ctx              context.Context
		projectCore      project.ICore
		brokerStore      brokerstore.IBrokerStore
		subscriptionCore subscription.ICore
		credentialCore   credentials.ICore
		manager          stream.IManager
		cache            cache2.ICache
		req              *metrov1.DeleteSubscriptionRequest
		subModel         *subscription.Model
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Delete invalid subscription",
			args: args{
				ctx:              ctx,
				projectCore:      mockProjectCore,
				brokerStore:      brokerStore,
				subscriptionCore: subscriptionCore,
				credentialCore:   mockCredentialsCore,
				manager:          manager,
				cache:            cache,
				req:              invalidReq,
				subModel:         getSubModel("invalid-sub", "projects/project123/topics/test-topic", "project123", "project123", "invalid-sub"),
			},
			wantErr: true,
		},
		{
			name: "Delete non-existent subscription",
			args: args{
				ctx:              ctx,
				projectCore:      mockProjectCore,
				brokerStore:      brokerStore,
				subscriptionCore: subscriptionCore,
				credentialCore:   mockCredentialsCore,
				manager:          manager,
				cache:            cache,
				req:              req,
				subModel:         getSubModel("projects/project123/subscriptions/sub", "projects/project123/topics/test-topic", "project123", "project123", "sub"),
			},
			wantErr: true,
		},
		{
			name: "Delete subscription with error from subscription core",
			args: args{
				ctx:              ctx,
				projectCore:      mockProjectCore,
				brokerStore:      brokerStore,
				subscriptionCore: subscriptionCore,
				credentialCore:   mockCredentialsCore,
				manager:          manager,
				cache:            cache,
				req:              req,
				subModel:         getSubModel(req.Subscription, "projects/project123/topics/test-topic", "project123", "project123", "sub-test"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newSubscriberServer(tt.args.projectCore, tt.args.brokerStore, tt.args.subscriptionCore, tt.args.credentialCore, tt.args.manager, tt.args.cache)
			subscriptionCore.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(arg0 context.Context, arg1 string) (*subscription.Model, error) {
				if tt.args.req.Subscription == tt.args.subModel.Name {
					return tt.args.subModel, nil
				} else {
					return nil, errors.New("Subscription doesn't exist")
				}
			}).AnyTimes()
			subscriptionCore.EXPECT().DeleteSubscription(gomock.Any(), gomock.Any()).DoAndReturn(func(arg0 context.Context, arg1 *subscription.Model) error {
				if tt.wantErr {
					return errors.New("Test error")
				} else {
					return nil
				}
			}).AnyTimes()
			_, err := server.DeleteSubscription(ctx, tt.args.req)
			assert.Equal(t, err != nil, tt.wantErr)
		})
	}
}

func TestSubscriberServer_DeleteSubscriptionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.DeleteSubscriptionRequest{
		Subscription: "projects/project123/subscriptions/testsub",
	}
	subModel := &subscription.Model{
		Name:                           req.Subscription,
		Topic:                          "projects/project123/topics/test-topic",
		ExtractedTopicProjectID:        "project123",
		ExtractedSubscriptionProjectID: "project123",
		ExtractedSubscriptionName:      "testsub",
	}
	subscriptionCore.EXPECT().Get(gomock.Any(), req.Subscription).Return(subModel, nil)
	subscriptionCore.EXPECT().DeleteSubscription(gomock.Any(), subModel).Times(1).Return(fmt.Errorf("error"))
	res, err := server.DeleteSubscription(ctx, req)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func TestSubscriberServer_ModifyAckDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.ModifyAckDeadlineRequest{
		Subscription:       "projects/project123/subscriptions/testsub",
		AckIds:             []string{validAckId},
		AckDeadlineSeconds: 1,
	}

	parsedReq, parseErr := stream.NewParsedModifyAckDeadlineRequest(req)
	assert.Nil(t, parseErr)

	manager.EXPECT().ModifyAcknowledgement(gomock.Any(), parsedReq).Return(nil)
	res, err := server.ModifyAckDeadline(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, &emptypb.Empty{}, res)
}

func TestSubscriberServer_ModifyAckDeadlineFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()
	req := &metrov1.ModifyAckDeadlineRequest{
		Subscription:       "projects/project123/subscriptions/testsub",
		AckIds:             []string{validAckId},
		AckDeadlineSeconds: 1,
	}

	parsedReq, parseErr := stream.NewParsedModifyAckDeadlineRequest(req)
	assert.Nil(t, parseErr)

	manager.EXPECT().ModifyAcknowledgement(gomock.Any(), parsedReq).Return(fmt.Errorf("error"))
	res, err := server.ModifyAckDeadline(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestSubscriberServer_StreamingPull(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)
}

func TestSubscriberServer_ListTopicSubscriptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()

	req := &metrov1.ListTopicSubscriptionsRequest{
		Topic: "projects/project_1/topics/topic_1",
	}

	subs := []*subscription.Model{
		{
			ExtractedTopicName:        "topic_1",
			ExtractedSubscriptionName: "sub_1",
		},
		{
			ExtractedTopicName:        "topic_1",
			ExtractedSubscriptionName: "sub_2",
		},
		{
			ExtractedTopicName:        "topic_1",
			ExtractedSubscriptionName: "sub_3",
		},
		{
			ExtractedTopicName:        "topic_2",
			ExtractedSubscriptionName: "sub_4",
		},
		{
			ExtractedTopicName:        "topic_2",
			ExtractedSubscriptionName: "sub_5",
		},
	}
	subscriptionCore.EXPECT().List(gomock.Any(), "subscriptions/project_1").Return(subs, nil)
	res, err := server.ListTopicSubscriptions(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(res.Subscriptions))
}

func TestSubscriberServer_ListTopicSubscriptionsFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()

	req := &metrov1.ListTopicSubscriptionsRequest{
		Topic: "projects/project_1/topics/topic_1",
	}

	subscriptionCore.EXPECT().List(gomock.Any(), "subscriptions/project_1").Return(nil, fmt.Errorf("error"))
	res, err := server.ListTopicSubscriptions(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestSubscriberServer_ListProjectSubscriptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()

	req := &metrov1.ListProjectSubscriptionsRequest{
		ProjectId: "project_1",
	}

	subs := []*subscription.Model{
		{
			ExtractedSubscriptionName: "sub_1",
		},
		{
			ExtractedSubscriptionName: "sub_2",
		},
		{
			ExtractedSubscriptionName: "sub_3",
		},
		{
			ExtractedSubscriptionName: "sub_4",
		},
		{
			ExtractedSubscriptionName: "sub_5",
		},
	}
	subscriptionCore.EXPECT().List(gomock.Any(), subscription.Prefix+req.ProjectId).Return(subs, nil)
	res, err := server.ListProjectSubscriptions(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(res.Subscriptions))
}

func TestSubscriberServer_ListProjectSubscriptionsFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)

	ctx := context.Background()

	req := &metrov1.ListProjectSubscriptionsRequest{
		ProjectId: "project_1",
	}

	subscriptionCore.EXPECT().List(gomock.Any(), subscription.Prefix+req.ProjectId).Return(nil, fmt.Errorf("error"))
	res, err := server.ListProjectSubscriptions(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

// TestSubscriberServer_GetSubscription: test the GetSubscription given subscription exist
func TestSubscriberServer_GetSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)
	ctx := context.Background()
	req := &metrov1.GetSubscriptionRequest{
		Name: "projects/project123/subscriptions/testsub",
	}
	sub := &subscription.Model{
		Name:  "projects/project123/subscriptions/testsub",
		Topic: "projects/project123/topics/test-topic",
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
			Attributes: map[string]string{
				"test_key": "test_value",
			},
			Credentials: &credentials.Model{
				Username: "test_user",
				Password: "",
			},
		},
		AckDeadlineSeconds: 10,
		DeadLetterPolicy: &subscription.DeadLetterPolicy{
			DeadLetterTopic:     "testsub-dlq",
			MaxDeliveryAttempts: 1,
		},
		RetryPolicy: &subscription.RetryPolicy{
			MinimumBackoff: 20,
			MaximumBackoff: 300,
		},
	}
	subscriptionCore.EXPECT().Get(gomock.Any(), req.GetName()).Times(1).Return(sub, nil)
	res, err := server.GetSubscription(ctx, req)
	pSub := subscription.ModelToSubscriptionProtoV1(sub)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.True(t, reflect.DeepEqual(res, pSub))
}

// TestSubscriberServer_GetSubscriptionFailure : test the GetSubscription given subscription does not exist
func TestSubscriberServer_GetSubscriptionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	cache := cachemock.NewMockICache(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager, cache)
	ctx := context.Background()
	req := &metrov1.GetSubscriptionRequest{
		Name: "projects/project123/subscriptions/testsub",
	}
	subscriptionCore.EXPECT().Get(gomock.Any(), req.GetName()).Times(1).Return(nil, fmt.Errorf("subscription does not exist"))
	res, err := server.GetSubscription(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func getSubModel(name, topic, projectID, subProjectID, subName string) *subscription.Model {
	return &subscription.Model{
		Name:                           name,
		Topic:                          topic,
		ExtractedTopicProjectID:        projectID,
		ExtractedSubscriptionProjectID: subProjectID,
		ExtractedSubscriptionName:      subName,
	}
}
