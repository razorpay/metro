package subscriber

import (
	"context"
	"time"

	"github.com/razorpay/metro/internal/brokerstore"
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
func (c *Core) NewSubscriber(ctx context.Context,
	subscriberID string,
	subscription *subscription.Model,
	timeoutInMs int,
	maxOutstandingMessages int64,
	maxOutstandingBytes int64,
	requestCh chan *PullRequest,
	ackCh chan *AckMessage,
	modAckCh chan *ModAckMessage) (ISubscriber, error) {

	topic := messagebroker.NormalizeTopicName(subscription.Topic)
	retryTopic := messagebroker.NormalizeTopicName(subscription.GetRetryTopic())
	dlqTopic := messagebroker.NormalizeTopicName(subscription.GetDeadLetterTopic())

	groupID := subscription.Name

	consumer, err := c.bs.GetConsumer(ctx, subscriberID, messagebroker.ConsumerClientOptions{Topics: []string{topic, retryTopic}, GroupID: groupID})
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: failed to create consumer", "error", err.Error())
		return nil, err
	}

	retryProducer, err := c.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{Topic: retryTopic, TimeoutMs: 500})
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: failed to create retry producer", "error", err.Error())
		return nil, err
	}

	dlqProducer, err := c.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{Topic: dlqTopic, TimeoutMs: 500})
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: failed to create dlq producer", "error", err.Error())
		return nil, err
	}

	subsCtx, cancelFunc := context.WithCancel(ctx)
	s := &Subscriber{
		subscription:           subscription.Name,
		topic:                  topic,
		retryTopic:             retryTopic,
		dlqTopic:               dlqTopic,
		subscriberID:           subscriberID,
		subscriptionCore:       c.subscriptionCore,
		requestChan:            requestCh,
		responseChan:           make(chan *metrov1.PullResponse),
		errChan:                make(chan error),
		closeChan:              make(chan struct{}),
		ackChan:                ackCh,
		modAckChan:             modAckCh,
		deadlineTicker:         time.NewTicker(deadlineTickerInterval),
		timeoutInMs:            timeoutInMs,
		consumer:               consumer,
		retryProducer:          retryProducer,
		dlqProducer:            dlqProducer,
		cancelFunc:             cancelFunc,
		maxOutstandingMessages: maxOutstandingMessages,
		maxOutstandingBytes:    maxOutstandingBytes,
		consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		ctx:                    subsCtx,
		bs:                     c.bs,
	}

	go s.Run(subsCtx)
	return s, nil
}
