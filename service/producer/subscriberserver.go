package producer

import (
	"context"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type subscriberserver struct {
	subscriptionCore subscription.ICore
}

func newSubscriberServer(subscriptionCore subscription.ICore) *subscriberserver {
	return &subscriberserver{subscriptionCore}
}

// CreateSubscription to create a new subscription
func (s subscriberserver) CreateSubscription(ctx context.Context, req *metrov1.Subscription) (*metrov1.Subscription, error) {
	logger.Ctx(ctx).Infow("received request to create subscription", "name", req.Name, "topic", req.Topic)
	m, err := subscription.GetValidatedModelForCreate(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	err = s.subscriptionCore.CreateSubscription(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return req, nil
}

// Acknowledge a message
func (s subscriberserver) Acknowledge(ctx context.Context, req *metrov1.AcknowledgeRequest) (*emptypb.Empty, error) {
	// TODO: Implement
	return new(emptypb.Empty), nil
}

// Pull messages
func (s subscriberserver) Pull(ctx context.Context, req *metrov1.PullRequest) (*metrov1.PullResponse, error) {
	logger.Ctx(ctx).Infow("received request to pull messages")
	// TODO: Implement
	return &metrov1.PullResponse{}, nil
}

// StreamingPull ...
func (s subscriberserver) StreamingPull(server metrov1.Subscriber_StreamingPullServer) error {
	// TODO: Implement
	return nil
}

// DeleteSubscription deletes a subscription
func (s subscriberserver) DeleteSubscription(ctx context.Context, req *metrov1.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("received request to delete subscription", "name", req.Subscription)
	m, err := subscription.GetValidatedModelForDelete(ctx, &metrov1.Subscription{Name: req.Subscription})
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	err = s.subscriptionCore.DeleteSubscription(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return &emptypb.Empty{}, nil
}
