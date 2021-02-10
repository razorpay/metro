package web

import (
	"context"
	"io"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type subscriberserver struct {
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ISubscriber
}

func newSubscriberServer(subscriptionCore subscription.ICore, subscriberCore subscriber.ISubscriber) *subscriberserver {
	return &subscriberserver{subscriptionCore, subscriberCore}
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
	logger.Ctx(ctx).Infow("received request to ack messages")
	return new(emptypb.Empty), nil
}

// Pull messages
func (s subscriberserver) Pull(ctx context.Context, req *metrov1.PullRequest) (*metrov1.PullResponse, error) {
	logger.Ctx(ctx).Infow("received request to pull messages")
	res, err := s.subscriberCore.Pull(ctx, &subscriber.PullRequest{req.Subscription, 0, 0}, 2)
	if err != nil {
		logger.Ctx(ctx).Errorw("pull response errored", "msg", err.Error())
		return nil, merror.ToGRPCError(err)
	}
	return &metrov1.PullResponse{ReceivedMessages: res.ReceivedMessages}, nil
}

// StreamingPull ...
func (s subscriberserver) StreamingPull(server metrov1.Subscriber_StreamingPullServer) error {
	ctx := server.Context()
	for {
		// exit if context is done
		// or continue
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Infow("returning after context is done")
			return ctx.Err()
		default:
		}
		// receive data from stream
		req, err := server.Recv()
		if err == io.EOF {
			// return will close stream from server side
			logger.Ctx(ctx).Info("EOF received from client")
			return nil
		}
		if err != nil {
			logger.Ctx(ctx).Infow("error received from client", "msg", err.Error())
			continue
		}
		// TODO: ping request fix
		if req.Subscription == "" {
			continue
		}
		logger.Ctx(ctx).Infow("received req", "req", req)
		pullReq, ackReq, ModAckReq, err := subscriber.FromProto(req)
		if err != nil {
			return merror.ToGRPCError(err)
		}
		err = s.subscriberCore.Acknowledge(ctx, ackReq)
		if err != nil {
			return merror.ToGRPCError(err)
		}
		err = s.subscriberCore.ModifyAckDeadline(ctx, ModAckReq)
		if err != nil {
			return merror.ToGRPCError(err)
		}
		res, err := s.subscriberCore.Pull(ctx, pullReq, 10)
		if err != nil {
			logger.Ctx(ctx).Errorw("pull response errored", "msg", err.Error())
			return merror.ToGRPCError(err)
		}
		err = server.Send(&metrov1.StreamingPullResponse{ReceivedMessages: res.ReceivedMessages})
		if err != nil {
			logger.Ctx(ctx).Errorw("error in send", "msg", err.Error())
			return merror.ToGRPCError(err)
		}
		logger.Ctx(ctx).Infow("StreamingPullResponse sent", "numOfMessages", len(res.ReceivedMessages))
	}

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

func (s subscriberserver) ModifyAckDeadline(ctx context.Context, in *metrov1.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("received request to modack messages")
	return &emptypb.Empty{}, nil
}
