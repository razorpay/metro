package web

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"

	"github.com/razorpay/metro/internal/credentials"

	"github.com/razorpay/metro/internal/interceptors"
	"github.com/razorpay/metro/internal/project"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/razorpay/metro/service/web/stream"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/emptypb"
)

type subscriberserver struct {
	projectCore      project.ICore
	brokerStore      brokerstore.IBrokerStore
	subscriptionCore subscription.ICore
	credentialCore   credentials.ICore
	psm              stream.IManager
}

func newSubscriberServer(projectCore project.ICore, brokerStore brokerstore.IBrokerStore, subscriptionCore subscription.ICore, credentialCore credentials.ICore, psm stream.IManager) *subscriberserver {
	return &subscriberserver{projectCore, brokerStore, subscriptionCore, credentialCore, psm}
}

// CreateSubscription to create a new subscription
func (s subscriberserver) CreateSubscription(ctx context.Context, req *metrov1.Subscription) (*metrov1.Subscription, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to create subscription", "name", req.Name, "topic", req.Topic)
	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.CreateSubscription")
	defer span.Finish()

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
	logger.Ctx(ctx).Infow("subscriberserver: received request to ack messages", "ack_req", req.String())

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.Acknowledge")
	defer span.Finish()

	parsedReq, parseErr := stream.NewParsedAcknowledgeRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("subscriberserver: error is parsing ack request", "request", req, "error", parseErr.Error())
		return nil, parseErr
	}

	err := s.psm.Acknowledge(ctx, parsedReq)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	return new(emptypb.Empty), nil
}

// Pull messages
func (s subscriberserver) Pull(ctx context.Context, req *metrov1.PullRequest) (*metrov1.PullResponse, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to pull messages", "pull_req", req.String())
	// non streaming pull not to be supported
	/*
		res, err := s.subscriberCore.Pull(ctx, &subscriber.PullRequest{req.Subscription, 0, 0}, 2, xid.New().String()) // TODO: fix
		if err != nil {
			logger.Ctx(ctx).Errorw("pull response errored", "msg", err.Error())
			return nil, merror.ToGRPCError(err)
		}
		return &metrov1.PullResponse{ReceivedMessages: res.ReceivedMessages}, nil

	*/
	return &metrov1.PullResponse{}, nil
}

// StreamingPull ...
func (s subscriberserver) StreamingPull(server metrov1.Subscriber_StreamingPullServer) error {
	// TODO: check if the requested subscription is push based and handle it the way pubsub does
	ctx := server.Context()
	errGroup := new(errgroup.Group)

	// the first request reaching this server path would always be to establish a new stream.
	// once established the active stream server instance will be held in pullstream and
	// periodically polled for new requests
	req, err := server.Recv()
	if err != nil {
		return err
	}

	parsedReq, parseErr := stream.NewParsedStreamingPullRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("subscriberserver: error is parsing pull request", "request", req, "error", parseErr.Error())
		return nil
	}

	// request to init a new stream
	if parsedReq.HasSubscription() {
		err := s.psm.CreateNewStream(server, parsedReq, errGroup)
		if err != nil {
			return merror.ToGRPCError(err)
		}
	} else {
		return merror.ToGRPCError(fmt.Errorf("subscription name empty"))
	}

	// ack and modack here for the first time
	// later it happens in stream handler
	if parsedReq.HasModifyAcknowledgement() {
		// request to modify acknowledgement deadlines
		// Nack indicated by modifying the deadline to zero
		// https://github.com/googleapis/google-cloud-go/blob/pubsub/v1.10.0/pubsub/iterator.go#L348
		err := s.psm.ModifyAcknowledgement(ctx, parsedReq)
		if err != nil {
			return merror.ToGRPCError(err)
		}
	}

	if parsedReq.HasAcknowledgement() {
		// request to acknowledge existing messages
		err := s.psm.Acknowledge(ctx, parsedReq)
		if err != nil {
			return merror.ToGRPCError(err)
		}
	}
	if err := errGroup.Wait(); err != nil {
		return err
	}
	return nil
}

// DeleteSubscription deletes a subscription
func (s subscriberserver) DeleteSubscription(ctx context.Context, req *metrov1.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to delete subscription", "name", req.Subscription)

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.DeleteSubscription")
	defer span.Finish()

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

func (s subscriberserver) ModifyAckDeadline(ctx context.Context, req *metrov1.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to modack messages", "mod_ack_req", req.String())

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.ModifyAckDeadline")
	defer span.Finish()

	parsedReq, parseErr := stream.NewParsedModifyAckDeadlineRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("subscriberserver: error is parsing modack request", "request", req, "error", parseErr.Error())
		return nil, parseErr
	}
	err := s.psm.ModifyAcknowledgement(ctx, parsedReq)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return new(emptypb.Empty), nil
}

func (s subscriberserver) AuthFuncOverride(ctx context.Context, _ string) (context.Context, error) {
	return interceptors.AppAuth(ctx, s.credentialCore)
}
