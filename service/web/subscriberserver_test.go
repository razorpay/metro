// +build unit

package web

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	mocks5 "github.com/razorpay/metro/internal/credentials/mocks/core"
	mocks4 "github.com/razorpay/metro/internal/project/mocks/core"
	"github.com/razorpay/metro/internal/subscription"
	mocks2 "github.com/razorpay/metro/internal/subscription/mocks/core"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/razorpay/metro/service/web/stream"
	mocks3 "github.com/razorpay/metro/service/web/stream/mocks/manager"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestSubscriberServer_UpdateSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name:  "projects/project123/subscriptions/testsub",
			Topic: "projects/project123/topics/test-topic",
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"push_config"},
		},
	}

	resp := &subscription.Model{
		Name:         req.Subscription.Name,
		Topic:        req.Subscription.Topic,
		PushEndpoint: "",
		Credentials:  nil,
	}

	m, paths, err := subscription.GetValidatedModelAndPathsForUpdate(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, paths, []string{"PushEndpoint", "Credentials"}, "update paths are not matching")

	subscriptionCore.EXPECT().UpdateSubscription(gomock.Any(), m, paths).Times(1).Return(resp, nil)
	updated, err := server.UpdateSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, updated, req.Subscription, "Update is not correct")
}

func TestSubscriberServer_UpdateSubscriptionError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name:  "projects/project123/subscriptions/testsub",
			Topic: "projects/project123/topics/test-topic",
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"push_config"},
		},
	}

	m, paths, err := subscription.GetValidatedModelAndPathsForUpdate(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, paths, []string{"PushEndpoint", "Credentials"}, "update paths are not matching")

	subscriptionCore.EXPECT().UpdateSubscription(gomock.Any(), m, paths).Times(1).Return(nil, fmt.Errorf("error"))
	_, err = server.UpdateSubscription(ctx, req)
	assert.NotNil(t, err)
}

func TestSubscriberServer_UpdateSubscriptionValidationError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name:  "projects/project123/subscriptions/testsub",
			Topic: "projects/project123/topics/test-topic",
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"push_caaanfig"},
		},
	}

	_, _, err := subscription.GetValidatedModelAndPathsForUpdate(ctx, req)
	assert.NotNil(t, err)

	_, err = server.UpdateSubscription(ctx, req)
	assert.NotNil(t, err)
}

func TestSubscriberServer_CreateSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

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
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

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
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

	ctx := context.Background()
	req := &metrov1.AcknowledgeRequest{
		Subscription: "projects/project321/subscriptions/testsub",
		AckIds:       []string{"a_a_a_0_0_1_1"},
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
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

	ctx := context.Background()
	req := &metrov1.AcknowledgeRequest{
		Subscription: "projects/project321/subscriptions/testsub",
		AckIds:       []string{"a_a_a_0_0_1_1"},
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
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

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
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

	ctx := context.Background()
	req := &metrov1.DeleteSubscriptionRequest{
		Subscription: "projects/project123/subscriptions/testsub",
	}

	m, err := subscription.GetValidatedModelForDelete(ctx, &metrov1.Subscription{Name: req.Subscription})
	assert.Nil(t, err)

	subscriptionCore.EXPECT().DeleteSubscription(gomock.Any(), m).Times(1).Return(nil)
	res, err := server.DeleteSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, &emptypb.Empty{}, res)
}

func TestSubscriberServer_DeleteSubscriptionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks4.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	mockCredentialsCore := mocks5.NewMockICore(ctrl)
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

	ctx := context.Background()
	req := &metrov1.DeleteSubscriptionRequest{
		Subscription: "projects/project123/subscriptions/testsub",
	}

	m, err := subscription.GetValidatedModelForDelete(ctx, &metrov1.Subscription{Name: req.Subscription})
	assert.Nil(t, err)

	subscriptionCore.EXPECT().DeleteSubscription(gomock.Any(), m).Times(1).Return(fmt.Errorf("error"))
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
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

	ctx := context.Background()
	req := &metrov1.ModifyAckDeadlineRequest{
		Subscription:       "projects/project123/subscriptions/testsub",
		AckIds:             []string{"a_a_a_1_1_1_1"},
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
	server := newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)

	ctx := context.Background()
	req := &metrov1.ModifyAckDeadlineRequest{
		Subscription:       "projects/project123/subscriptions/testsub",
		AckIds:             []string{"a_a_a_1_1_1_1"},
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
	newSubscriberServer(mockProjectCore, brokerStore, subscriptionCore, mockCredentialsCore, manager)
}
