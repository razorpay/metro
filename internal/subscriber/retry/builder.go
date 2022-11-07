package retry

import (
	"sync"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/cache"
)

// Builder ...
type Builder interface {
	WithBackoff(subscription.Backoff) Builder
	WithCache(ch cache.ICache) Builder
	WithIntervalFinder(finder subscription.IntervalFinder) Builder
	WithBrokerStore(store brokerstore.IBrokerStore) Builder
	WithSubscription(subs *subscription.Model) Builder
	WithMessageHandler(handler MessageHandler) Builder
	WithSubscriberID(subscriberID string) Builder
	WithErrChan(chan error) Builder
	WithTopicCore(topicCore topic.ICore) Builder
	Build() IRetrier
}

// NewRetrierBuilder ...
func NewRetrierBuilder() Builder {
	return &Retrier{delayConsumers: sync.Map{}}
}

// Build ...
func (retrier *Retrier) Build() IRetrier {
	return retrier
}

// WithBackoff ...
func (retrier *Retrier) WithBackoff(backoff subscription.Backoff) Builder {
	retrier.backoff = backoff
	return retrier
}

// WithCache ...
func (retrier *Retrier) WithCache(ch cache.ICache) Builder {
	retrier.ch = ch
	return retrier
}

// WithIntervalFinder ...
func (retrier *Retrier) WithIntervalFinder(finder subscription.IntervalFinder) Builder {
	retrier.finder = finder
	return retrier
}

// WithBrokerStore ...
func (retrier *Retrier) WithBrokerStore(store brokerstore.IBrokerStore) Builder {
	retrier.bs = store
	return retrier
}

// WithSubscription ...
func (retrier *Retrier) WithSubscription(subs *subscription.Model) Builder {
	retrier.subs = subs
	return retrier
}

// WithMessageHandler ...
func (retrier *Retrier) WithMessageHandler(handler MessageHandler) Builder {
	retrier.handler = handler
	return retrier
}

// WithSubscriberID ...
func (retrier *Retrier) WithSubscriberID(subscriberID string) Builder {
	retrier.subscriberID = subscriberID
	return retrier
}

// WithErrChan ...
func (retrier *Retrier) WithErrChan(errChan chan error) Builder {
	retrier.errChan = errChan
	return retrier
}

// WithTopicCore ...
func (retrier *Retrier) WithTopicCore(topicCore topic.ICore) Builder {
	retrier.topicCore = topicCore
	return retrier
}
