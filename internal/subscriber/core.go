package subscriber

import (
	"context"
	"strings"

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
	return &Core{bs, subscriptionCore}
}

// NewSubscriber initiates a new subscriber for a given topic
func (c *Core) NewSubscriber(ctx context.Context, id string, subscription string, timeoutInSec int, maxOutstandingMessages int64, maxOutstandingBytes int64) (ISubscriber, error) {
	t, err := c.subscriptionCore.GetTopicFromSubscriptionName(ctx, subscription)
	if err != nil {
		return nil, err
	}
	consumer, err := c.bs.GetConsumer(ctx, id, messagebroker.ConsumerClientOptions{Topic: strings.Replace(t, "/", "_", -1), GroupID: subscription})
	if err != nil {
		return nil, err
	}
	subsCtx, cancelFunc := context.WithCancel(ctx)
	s := &Subscriber{c.bs,
		c.subscriptionCore,
		make(chan *PullRequest),
		make(chan metrov1.PullResponse),
		make(chan error),
		make(chan struct{}),
		timeoutInSec,
		consumer,
		cancelFunc,
		maxOutstandingMessages,
		maxOutstandingBytes,
	}
	go s.Run(subsCtx)
	return s, nil
}
