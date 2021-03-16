package subscriber

import (
	"context"
	"strings"

	topic2 "github.com/razorpay/metro/internal/topic"

	"github.com/google/uuid"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// ICore is interface over subscribers core
type ICore interface {
	NewSubscriber(ctx context.Context, id string, subscription string, timeoutInSec int, maxOutstandingMessages int64, maxOutstandingBytes int64) (ISubscriber, error)
}

// Core implements ISubscriber
type Core struct {
	bs               brokerstore.IBrokerStore
	subscriptionCore subscription.ICore
}

// NewCore returns a new core
func NewCore(bs brokerstore.IBrokerStore, subscriptionCore subscription.ICore) *Core {
	return &Core{bs: bs, subscriptionCore: subscriptionCore}
}

// NewSubscriber initiates a new subscriber for a given topic
func (c *Core) NewSubscriber(ctx context.Context, id string, subscription string, timeoutInSec int, maxOutstandingMessages int64, maxOutstandingBytes int64) (ISubscriber, error) {
	t, err := c.subscriptionCore.GetTopicFromSubscriptionName(ctx, subscription)
	if err != nil {
		return nil, err
	}

	subscriberID := uuid.New().String()

	topic := strings.Replace(t, "/", "_", -1)
	groupID := subscription
	consumer, err := c.bs.GetConsumer(ctx, groupID, messagebroker.ConsumerClientOptions{Topic: topic, GroupID: groupID})
	if err != nil {
		return nil, err
	}

	// make sure retry topic creation is taken care during the primary topic creation flow
	retryTopic := topic + topic2.RetryTopicSuffix
	retryGroupID := subscription + topic2.RetryTopicSuffix
	retryConsumer, err := c.bs.GetConsumer(ctx, retryGroupID, messagebroker.ConsumerClientOptions{Topic: retryTopic, GroupID: retryGroupID})
	if err != nil {
		return nil, err
	}

	retryProducer, err := c.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{Topic: retryTopic, TimeoutSec: 50})
	if err != nil {
		return nil, err
	}

	subsCtx, cancelFunc := context.WithCancel(ctx)
	s := &Subscriber{
		subscription:           subscription,
		topic:                  topic,
		retryTopic:             retryTopic,
		subscriberID:           subscriberID,
		subscriptionCore:       c.subscriptionCore,
		requestChan:            make(chan *PullRequest),
		responseChan:           make(chan metrov1.PullResponse),
		errChan:                make(chan error),
		closeChan:              make(chan struct{}),
		ackChan:                make(chan *AckMessage),
		modAckChan:             make(chan *ModAckMessage),
		deadlineTickerChan:     make(chan bool),
		timeoutInSec:           timeoutInSec,
		consumer:               consumer,
		retryConsumer:          retryConsumer,
		retryProducer:          retryProducer,
		cancelFunc:             cancelFunc,
		maxOutstandingMessages: maxOutstandingMessages,
		maxOutstandingBytes:    maxOutstandingBytes,
		consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		ctx:                    subsCtx,
	}

	go s.Run(subsCtx)
	return s, nil
}
