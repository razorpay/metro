package retry

import (
	"sync"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
)

// Builder ...
type Builder interface {
	WithBackoff(Backoff) Builder
	WithIntervalFinder(finder IntervalFinder) Builder
	WithBrokerStore(store brokerstore.IBrokerStore) Builder
	WithSubscription(subs *subscription.Model) Builder
	WithMessageHandler(handler MessageHandler) Builder
	WithSubscriberID(subscriberID string) Builder
	WithPartition(partition int) Builder
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
func (retrier *Retrier) WithBackoff(backoff Backoff) Builder {
	retrier.backoff = backoff
	return retrier
}

// WithIntervalFinder ...
func (retrier *Retrier) WithIntervalFinder(finder IntervalFinder) Builder {
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

// WithPartition ...
func (retrier *Retrier) WithPartition(partition int) Builder {
	retrier.partition = partition
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
