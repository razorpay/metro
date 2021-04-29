package subscriber

import (
	"context"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// ICore is interface over subscribers core
type ICore interface {
	NewSubscriber(ctx context.Context, id string, subscription string, timeoutInMs int, maxOutstandingMessages int64, maxOutstandingBytes int64) (ISubscriber, error)
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
func (c *Core) NewSubscriber(ctx context.Context, subscriberID string, subscription string, timeoutInMs int, maxOutstandingMessages int64, maxOutstandingBytes int64) (ISubscriber, error) {
	subModel, err := c.subscriptionCore.Get(ctx, subscription)
	if err != nil {
		return nil, err
	}

	topic := messagebroker.NormalizeTopicName(subModel.Topic)
	retryTopic := messagebroker.NormalizeTopicName(subModel.GetRetryTopic())
	dlqTopic := messagebroker.NormalizeTopicName(subModel.GetDeadLetterTopic())

	groupID := subscription

	consumer, err := c.bs.GetConsumer(ctx, subscriberID, messagebroker.ConsumerClientOptions{Topics: []string{topic, retryTopic}, GroupID: groupID})
	if err != nil {
		return nil, err
	}

	retryProducer, err := c.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{Topic: retryTopic, TimeoutMs: 50})
	if err != nil {
		return nil, err
	}

	dlqProducer, err := c.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{Topic: dlqTopic, TimeoutMs: 50})
	if err != nil {
		return nil, err
	}

	subsCtx, cancelFunc := context.WithCancel(ctx)
	s := &Subscriber{
		subscription:           subscription,
		topic:                  topic,
		retryTopic:             retryTopic,
		dlqTopic:               dlqTopic,
		subscriberID:           subscriberID,
		subscriptionCore:       c.subscriptionCore,
		requestChan:            make(chan *PullRequest),
		responseChan:           make(chan metrov1.PullResponse),
		errChan:                make(chan error),
		closeChan:              make(chan struct{}),
		ackChan:                make(chan *AckMessage),
		modAckChan:             make(chan *ModAckMessage),
		deadlineTickerChan:     make(chan bool),
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
