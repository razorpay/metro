package web

import (
	"context"

	"github.com/imdario/mergo"
	"github.com/mennanov/fmutils"
	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/cache"
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
	ch               cache.ICache
}

func newSubscriberServer(projectCore project.ICore, brokerStore brokerstore.IBrokerStore, subscriptionCore subscription.ICore, credentialCore credentials.ICore, psm stream.IManager, ch cache.ICache) *subscriberserver {
	return &subscriberserver{projectCore, brokerStore, subscriptionCore, credentialCore, psm, ch}
}

// CreateSubscription to create a new subscription
func (s subscriberserver) CreateSubscription(ctx context.Context, req *metrov1.Subscription) (*metrov1.Subscription, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to create subscription", "name", req.Name, "topic", req.Topic)
	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.CreateSubscription", opentracing.Tags{
		"topic":        req.Topic,
		"subscription": req.Name,
	})
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

// UpdateSubscription updates a given subscription
func (s subscriberserver) UpdateSubscription(ctx context.Context, req *metrov1.UpdateSubscriptionRequest) (*metrov1.Subscription, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to update subscription", "name", req.Subscription.Name, "topic", req.Subscription.Topic)
	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.UpdateSubscription")
	defer span.Finish()

	if err := subscription.ValidateUpdateSubscriptionRequest(ctx, req); err != nil {
		return nil, merror.ToGRPCError(err)
	}

	sub, err := s.subscriptionCore.Get(ctx, req.Subscription.Name)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	existingSubscription := subscription.ModelToSubscriptionProtoV1(sub)
	fmutils.Prune(existingSubscription, req.UpdateMask.Paths)
	fmutils.Filter(req.Subscription, req.UpdateMask.Paths)

	if err = mergo.Merge(existingSubscription, req.Subscription); err != nil {
		return nil, merror.ToGRPCError(err)
	}

	patchedModel, err := subscription.GetValidatedModelForUpdate(ctx, existingSubscription)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	if err = s.subscriptionCore.UpdateSubscription(ctx, patchedModel); err != nil {
		return nil, merror.ToGRPCError(err)
	}
	patchedProto := subscription.ModelToSubscriptionProtoV1(patchedModel)
	return patchedProto, nil
}

// Acknowledge a message
func (s subscriberserver) Acknowledge(ctx context.Context, req *metrov1.AcknowledgeRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to ack messages", "ack_req", req.String())

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.Acknowledge", opentracing.Tags{
		"subscription": req.Subscription,
		"ack_ids":      req.AckIds,
	})
	defer span.Finish()

	parsedReq, parseErr := stream.NewParsedAcknowledgeRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("subscriberserver: error is parsing ack request", "request", req, "error", parseErr.Error())
		return nil, merror.ToGRPCError(parseErr)
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

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.Pull", opentracing.Tags{
		"subscription": req.Subscription,
	})
	defer span.Finish()

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
		err := s.psm.CreateNewStream(server, parsedReq, errGroup, s.ch)
		if err != nil {
			return merror.ToGRPCError(err)
		}
	} else {
		return merror.New(merror.InvalidArgument, "subscription name empty").ToGRPCError()
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

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.DeleteSubscription", opentracing.Tags{
		"subscription": req.Subscription,
	})
	defer span.Finish()

	m, err := subscription.GetValidatedModelForDelete(ctx, &metrov1.Subscription{Name: req.Subscription})
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	sub, err := s.subscriptionCore.Get(ctx, req.Subscription)
	if err != nil {
		return &emptypb.Empty{}, merror.Newf(merror.NotFound, "Subscription does not exist")
	}
	m.Topic = sub.Topic
	m.ExtractedSubscriptionName = sub.ExtractedSubscriptionName
	m.ExtractedSubscriptionProjectID = sub.ExtractedSubscriptionProjectID
	m.ExtractedTopicProjectID = sub.ExtractedTopicProjectID
	err = s.subscriptionCore.DeleteSubscription(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s subscriberserver) ModifyAckDeadline(ctx context.Context, req *metrov1.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to modack messages", "mod_ack_req", req.String())

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.ModifyAckDeadline", opentracing.Tags{
		"subscription": req.Subscription,
		"ack_ids":      req.AckIds,
	})
	defer span.Finish()

	parsedReq, parseErr := stream.NewParsedModifyAckDeadlineRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("subscriberserver: error is parsing modack request", "request", req, "error", parseErr.Error())
		return nil, merror.ToGRPCError(parseErr)
	}
	err := s.psm.ModifyAcknowledgement(ctx, parsedReq)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return new(emptypb.Empty), nil
}

func (s subscriberserver) ListTopicSubscriptions(ctx context.Context, req *metrov1.ListTopicSubscriptionsRequest) (*metrov1.ListTopicSubscriptionsResponse, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to list topic subscriptions", "topic", req.Topic)

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.ListTopicSubscriptions", opentracing.Tags{
		"topic": req.Topic,
	})
	defer span.Finish()

	p, t, err := topic.ExtractTopicMetaAndValidate(ctx, req.Topic)

	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	subs, err := s.subscriptionCore.List(ctx, subscription.Prefix+p)

	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	res := []string{}
	for _, sub := range subs {
		if sub.ExtractedTopicName == t {
			res = append(res, sub.ExtractedSubscriptionName)
		}
	}

	return &metrov1.ListTopicSubscriptionsResponse{Subscriptions: res}, nil
}

func (s subscriberserver) ListProjectSubscriptions(ctx context.Context,
	req *metrov1.ListProjectSubscriptionsRequest) (*metrov1.ListProjectSubscriptionsResponse, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to list project subscriptions", "project_id", req.ProjectId)

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.ListProjectSubscriptions", opentracing.Tags{
		"project": req.ProjectId,
	})
	defer span.Finish()

	subs, err := s.subscriptionCore.List(ctx, subscription.Prefix+req.ProjectId)

	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	res := []string{}
	for _, sub := range subs {
		res = append(res, sub.ExtractedSubscriptionName)
	}

	return &metrov1.ListProjectSubscriptionsResponse{Subscriptions: res}, nil
}

// AuthFuncOverride - Override function called by the auth interceptor
func (s subscriberserver) AuthFuncOverride(ctx context.Context, fullMethodName string, req interface{}) (context.Context, error) {
	return authRequest(ctx, s.credentialCore, fullMethodName, req)
}

// GetSubscription get subscription details by name
func (s subscriberserver) GetSubscription(ctx context.Context, req *metrov1.GetSubscriptionRequest) (*metrov1.Subscription, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to get subscription details", "subscription_name", req.GetName())
	subs, err := s.subscriptionCore.Get(ctx, req.GetName())
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriberserver: error while getting subscription details from subscription core", "subscription_name", req.GetName())
		return nil, err
	}
	// set password to empty since password should not be present in the response
	if subs.IsPush() && subs.PushConfig.Credentials != nil {
		subs.PushConfig.Credentials.Password = ""
	}
	subs.PushConfig.PushEndpoint = subs.GetRedactedPushEndpoint()
	return subscription.ModelToSubscriptionProtoV1(subs), nil
}
