package consumeplane

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/consumer"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/razorpay/metro/service/web/stream"
	"google.golang.org/protobuf/types/known/emptypb"
)

type consumeplaneserver struct {
	brokerStore      brokerstore.IBrokerStore
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ICore
	manager          consumer.ILifecycle
}

func newConsumePlaneServer(brokerStore brokerstore.IBrokerStore, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore, mgr consumer.ILifecycle) *consumeplaneserver {

	return &consumeplaneserver{
		brokerStore,
		subscriptionCore,
		subscriberCore,
		mgr,
	}
}

// Acknowledge a message
func (c consumeplaneserver) Acknowledge(ctx context.Context, req *metrov1.AcknowledgeRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("consumeplaneserver: received request to ack messages", "ack_req", req.String())

	span, ctx := opentracing.StartSpanFromContext(ctx, "ConsumePlaneServer.Acknowledge", opentracing.Tags{
		"subscription": req.Subscription,
		"ack_ids":      req.AckIds,
	})
	defer span.Finish()

	parsedReq, parseErr := stream.NewParsedAcknowledgeRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("consumeplaneserver: error is parsing ack request", "request", req, "error", parseErr.Error())
		return nil, merror.ToGRPCError(parseErr)
	}
	partitionAckMsgs := make(map[int][]*subscriber.AckMessage, 0)
	for _, ack := range parsedReq.AckMessages {
		partitionAckMsgs[int(ack.Partition)] = append(partitionAckMsgs[int(ack.Partition)], ack)
	}

	for partition, ackMsgs := range partitionAckMsgs {
		consumer, err := c.manager.GetConsumer(ctx, parsedReq.Subscription, partition)
		if err != nil {
			logger.Ctx(ctx).Errorw("consumeplaneserver: error is fetching consumer for fetch request", "request", req, "error", parseErr.Error())
			return nil, err
		}
		consumer.Acknowledge(ctx, ackMsgs)

	}

	return new(emptypb.Empty), nil
}

// Fetch messages from the topic-partition
func (c consumeplaneserver) Fetch(ctx context.Context, req *metrov1.FetchRequest) (*metrov1.PullResponse, error) {
	logger.Ctx(ctx).Infow("consumeplaneserver: received request to pull messages", "pull_req", req.String())
	span, ctx := opentracing.StartSpanFromContext(ctx, "ConsumePlaneServer.Pull", opentracing.Tags{
		"subscription": req.Subscription,
	})

	parsedReq, parseErr := consumer.NewParsedFetchRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("consumeplaneserver: error is parsing pull request", "request", req, "error", parseErr.Error())
		return nil, parseErr
	}
	defer span.Finish()
	consumer, err := c.manager.GetConsumer(ctx, parsedReq.Subscription, parsedReq.Partition)
	if err != nil {
		logger.Ctx(ctx).Errorw("consumeplaneserver: error in fetching consumer for fetch request", "subscription", parsedReq.Subscription)
		return &metrov1.PullResponse{}, err
	}
	res, err := consumer.Fetch(ctx, int(req.MaxMessages))
	if err != nil {
		logger.Ctx(ctx).Errorw("consumeplaneserver: Failed to fetch messages", "err", err.Error())
		return &metrov1.PullResponse{}, err
	}
	return res, nil
}

// ModifyAckDeadline updates dealine for the given ackMessages
func (c consumeplaneserver) ModifyAckDeadline(ctx context.Context, req *metrov1.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("consumeplaneserver: received request to modack messages", "mod_ack_req", req.String())

	span, ctx := opentracing.StartSpanFromContext(ctx, "ConsumePlaneServer.ModifyAckDeadline", opentracing.Tags{
		"subscription": req.Subscription,
		"ack_ids":      req.AckIds,
	})
	defer span.Finish()

	parsedReq, parseErr := consumer.NewParsedModifyAckDeadlineRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("consumeplaneserver: error is parsing modack request", "request", req, "error", parseErr.Error())
		return nil, merror.ToGRPCError(parseErr)
	}
	partitionAckMsgs := make(map[int][]*subscriber.AckMessage, 0)
	for _, ack := range parsedReq.AckMessages {
		partitionAckMsgs[int(ack.Partition)] = append(partitionAckMsgs[int(ack.Partition)], ack)
	}

	for partition, ackMsgs := range partitionAckMsgs {
		consumer, err := c.manager.GetConsumer(ctx, parsedReq.Subscription, partition)
		if err != nil {
			logger.Ctx(ctx).Errorw("consumeplaneserver: error is fetching consumer for fetch request", "request", req, "error", parseErr.Error())
			return nil, err
		}
		consumer.ModifyAckDeadline(ctx, ackMsgs)

	}
	return new(emptypb.Empty), nil
}
