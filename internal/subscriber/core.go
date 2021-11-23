package subscriber

import (
	"context"
	"time"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscriber/retry"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// ICore is interface over subscribers core
type ICore interface {
	NewSubscriber(ctx context.Context, id string, subscription *subscription.Model, timeoutInMs int, maxOutstandingMessages int64, maxOutstandingBytes int64,
		requestCh chan *PullRequest, ackCh chan *AckMessage, modAckCh chan *ModAckMessage) (ISubscriber, error)
}

// Core implements ICore
type Core struct {
	bs               brokerstore.IBrokerStore
	subscriptionCore subscription.ICore
	offsetCore       offset.ICore
}

// NewCore returns a new subscriber core
func NewCore(bs brokerstore.IBrokerStore, subscriptionCore subscription.ICore, offsetCore offset.ICore) ICore {
	return &Core{bs: bs, subscriptionCore: subscriptionCore, offsetCore: offsetCore}
}

// NewSubscriber initiates a new subscriber for a given topic
func (c *Core) NewSubscriber(ctx context.Context,
	subscriberID string,
	subscription *subscription.Model,
	timeoutInMs int,
	maxOutstandingMessages int64,
	maxOutstandingBytes int64,
	requestCh chan *PullRequest,
	ackCh chan *AckMessage,
	modAckCh chan *ModAckMessage) (ISubscriber, error) {

	groupID := subscription.Name

	consumer, err := c.bs.GetConsumer(ctx, messagebroker.ConsumerClientOptions{Topics: []string{subscription.Topic, subscription.GetRetryTopic()}, GroupID: groupID, GroupInstanceID: subscriberID})
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: failed to create consumer", "error", err.Error())
		return nil, err
	}

	subsCtx, cancelFunc := context.WithCancel(ctx)
	// using the subscriber ctx for retrier as well. This way when the ctx() for subscribers is done,
	// all the delay-consumers spawned within retrier would also get marked as done.
	var retrier retry.IRetrier
	if subscription.RetryPolicy != nil && subscription.DeadLetterPolicy != nil {

		retrier = retry.NewRetrierBuilder().
			WithSubscription(subscription).
			WithBrokerStore(c.bs).
			WithBackoff(retry.NewExponentialWindowBackoff()).
			WithIntervalFinder(retry.NewClosestIntervalWithCeil()).
			WithMessageHandler(retry.NewPushToPrimaryRetryTopicHandler(c.bs)).
			WithSubscriberID(subscriberID).
			Build()

		err = retrier.Start(subsCtx)
		if err != nil {
			return nil, err
		}
	}

	s := &Subscriber{
		subscription:           subscription,
		topic:                  subscription.Topic,
		subscriberID:           subscriberID,
		subscriptionCore:       c.subscriptionCore,
		offsetCore:             c.offsetCore,
		requestChan:            requestCh,
		responseChan:           make(chan *metrov1.PullResponse),
		errChan:                make(chan error, 1000),
		closeChan:              make(chan struct{}),
		ackChan:                ackCh,
		modAckChan:             modAckCh,
		deadlineTicker:         time.NewTicker(deadlineTickerInterval),
		timeoutInMs:            timeoutInMs,
		consumer:               consumer,
		cancelFunc:             cancelFunc,
		maxOutstandingMessages: maxOutstandingMessages,
		maxOutstandingBytes:    maxOutstandingBytes,
		consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		ctx:                    subsCtx,
		bs:                     c.bs,
		retrier:                retrier,
	}

	go s.Run(subsCtx)
	return s, nil
}
