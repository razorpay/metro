package subscriber

import (
	"context"
	"time"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscriber/retry"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
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

	consumer, err := NewConsumerManager(ctx, c.bs, timeoutInMs, subscriberID, subscription.Name, subscription.Topic, subscription.GetRetryTopic())
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

	var subImpl Implementation
	if subscription.EnableMessageOrdering {

	} else {
		subImpl = &BasicImplementation{
			maxOutstandingMessages: maxOutstandingMessages,
			maxOutstandingBytes:    maxOutstandingBytes,
			topic:                  subscription.Topic,
			subscriberID:           subscriberID,
			consumer:               consumer,
			offsetCore:             c.offsetCore,
			retrier:                retrier,
			ctx:                    subsCtx,
			subscription:           subscription,
			consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		}
	}

	s := &Subscriber{
		subscription: subscription,
		topic:        subscription.Topic,
		subscriberID: subscriberID,
		// offsetCore:             c.offsetCore,
		requestChan:    requestCh,
		responseChan:   make(chan *metrov1.PullResponse),
		errChan:        make(chan error, 1000),
		closeChan:      make(chan struct{}),
		ackChan:        ackCh,
		modAckChan:     modAckCh,
		deadlineTicker: time.NewTicker(deadlineTickerInterval),
		consumer:       consumer,
		cancelFunc:     cancelFunc,
		// maxOutstandingMessages: maxOutstandingMessages,
		// maxOutstandingBytes:    maxOutstandingBytes,
		// consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		ctx: subsCtx,
		// bs:                     c.bs,
		retrier: retrier,
		// pausedMessages:         make([]messagebroker.ReceivedMessage, 0),
		// sequenceManager:        NewOffsetSequenceManager(ctx, c.offsetCore),
		subscriberImpl: subImpl,
	}

	go s.Run(subsCtx)
	return s, nil
}
