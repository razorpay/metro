// +build unit

package web

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/subscription"
	mocks2 "github.com/razorpay/metro/internal/subscription/mocks/core"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/razorpay/metro/service/web/stream"
	mocks3 "github.com/razorpay/metro/service/web/stream/mocks/manager"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSubscriberServer_CreateSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	server := newSubscriberServer(brokerStore, subscriptionCore, manager)

	ctx := context.Background()
	req := &metrov1.Subscription{
		Name:  "projects/project321/subscriptions/testsub",
		Topic: "projects/project123/topics/test-topic",
	}

	m, err := subscription.GetValidatedModelForCreate(ctx, req)
	assert.Nil(t, err)

	subscriptionCore.EXPECT().CreateSubscription(ctx, m).Times(1).Return(nil)
	res, err := server.CreateSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, res, req)
}

func TestSubscriberServer_CreateSubscriptionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	server := newSubscriberServer(brokerStore, subscriptionCore, manager)

	ctx := context.Background()
	req := &metrov1.Subscription{
		Name:  "projects/project321/subscriptions/testsub",
		Topic: "projects/project123/topics/test-topic",
	}

	m, err := subscription.GetValidatedModelForCreate(ctx, req)
	assert.Nil(t, err)

	subscriptionCore.EXPECT().CreateSubscription(ctx, m).Times(1).Return(fmt.Errorf("error"))
	res, err := server.CreateSubscription(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestSubscriberServer_Acknowledge(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	server := newSubscriberServer(brokerStore, subscriptionCore, manager)

	ctx := context.Background()
	req := &metrov1.AcknowledgeRequest{
		Subscription: "projects/project321/subscriptions/testsub",
		AckIds:       []string{"a_a_a_0_0_1_1"},
	}
	parsedReq, parseErr := stream.NewParsedAcknowledgeRequest(req)
	assert.Nil(t, parseErr)

	manager.EXPECT().Acknowledge(ctx, parsedReq).Times(1).Return(nil)

	res, err := server.Acknowledge(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, new(emptypb.Empty), res)
}

func TestSubscriberServer_AcknowledgeFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	server := newSubscriberServer(brokerStore, subscriptionCore, manager)

	ctx := context.Background()
	req := &metrov1.AcknowledgeRequest{
		Subscription: "projects/project321/subscriptions/testsub",
		AckIds:       []string{"a_a_a_0_0_1_1"},
	}
	parsedReq, parseErr := stream.NewParsedAcknowledgeRequest(req)
	assert.Nil(t, parseErr)

	manager.EXPECT().Acknowledge(ctx, parsedReq).Times(1).Return(fmt.Errorf("error"))

	res, err := server.Acknowledge(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestSubscriberServer_Pull(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	server := newSubscriberServer(brokerStore, subscriptionCore, manager)

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
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	server := newSubscriberServer(brokerStore, subscriptionCore, manager)

	ctx := context.Background()
	req := &metrov1.DeleteSubscriptionRequest{
		Subscription: "projects/project123/subscriptions/testsub",
	}

	m, err := subscription.GetValidatedModelForDelete(ctx, &metrov1.Subscription{Name: req.Subscription})
	assert.Nil(t, err)

	subscriptionCore.EXPECT().DeleteSubscription(ctx, m).Times(1).Return(nil)
	res, err := server.DeleteSubscription(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, &emptypb.Empty{}, res)
}

func TestSubscriberServer_DeleteSubscriptionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	server := newSubscriberServer(brokerStore, subscriptionCore, manager)

	ctx := context.Background()
	req := &metrov1.DeleteSubscriptionRequest{
		Subscription: "projects/project123/subscriptions/testsub",
	}

	m, err := subscription.GetValidatedModelForDelete(ctx, &metrov1.Subscription{Name: req.Subscription})
	assert.Nil(t, err)

	subscriptionCore.EXPECT().DeleteSubscription(ctx, m).Times(1).Return(fmt.Errorf("error"))
	res, err := server.DeleteSubscription(ctx, req)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func TestSubscriberServer_ModifyAckDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	server := newSubscriberServer(brokerStore, subscriptionCore, manager)

	ctx := context.Background()
	req := &metrov1.ModifyAckDeadlineRequest{
		Subscription:       "projects/project123/subscriptions/testsub",
		AckIds:             []string{"a_a_a_1_1_1_1"},
		AckDeadlineSeconds: 1,
	}

	parsedReq, parseErr := stream.NewParsedModifyAckDeadlineRequest(req)
	assert.Nil(t, parseErr)

	manager.EXPECT().ModifyAcknowledgement(ctx, parsedReq).Return(nil)
	res, err := server.ModifyAckDeadline(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, &emptypb.Empty{}, res)
}

func TestSubscriberServer_ModifyAckDeadlineFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	server := newSubscriberServer(brokerStore, subscriptionCore, manager)

	ctx := context.Background()
	req := &metrov1.ModifyAckDeadlineRequest{
		Subscription:       "projects/project123/subscriptions/testsub",
		AckIds:             []string{"a_a_a_1_1_1_1"},
		AckDeadlineSeconds: 1,
	}

	parsedReq, parseErr := stream.NewParsedModifyAckDeadlineRequest(req)
	assert.Nil(t, parseErr)

	manager.EXPECT().ModifyAcknowledgement(ctx, parsedReq).Return(fmt.Errorf("error"))
	res, err := server.ModifyAckDeadline(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestSubscriberServer_StreamingPull(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	subscriptionCore := mocks2.NewMockICore(ctrl)
	manager := mocks3.NewMockIManager(ctrl)
	newSubscriberServer(brokerStore, subscriptionCore, manager)
}
