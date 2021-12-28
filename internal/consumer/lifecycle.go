package consumer

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	"golang.org/x/sync/errgroup"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
)

// ILifecycle interface defines lifecycle manager methods
type ILifecycle interface {
	GetConsumer(ctx context.Context, sub string, partition int) (*Consumer, error)
	CloseConsumer(ctx context.Context, computedHash int) error
	Run() error
}

// Manager ...
type Manager struct {
	consumers        map[int]*Consumer
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ICore
	subCache         []*subscription.Model
	cleanupCh        chan cleanupMessage
	registry         registry.IRegistry
	replicas         int
	ordinalID        int
	bs               brokerstore.IBrokerStore
	mutex            *sync.Mutex
	ctx              context.Context
	subWatchData     chan *struct{}
}

// NewLifecycleManager ...
func NewLifecycleManager(ctx context.Context, replicas int, ordinalID int, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore, bs brokerstore.IBrokerStore, r registry.IRegistry) (ILifecycle, error) {

	allSubs, err := subscriptionCore.List(ctx, subscription.Prefix)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching new subscription list", "error", err)
		return nil, err
	}

	// Filter Pull Subscriptions
	var pullSubs []*subscription.Model
	for _, sub := range allSubs {
		if !sub.IsPush() {
			pullSubs = append(pullSubs, sub)
		}
	}

	mgr := &Manager{
		consumers:        make(map[int]*Consumer),
		subscriptionCore: subscriptionCore,
		subscriberCore:   subscriberCore,
		registry:         r,
		bs:               bs,
		replicas:         replicas,
		ordinalID:        ordinalID,
		mutex:            &sync.Mutex{},
		ctx:              ctx,
		subCache:         pullSubs,
		subWatchData:     make(chan *struct{}),
	}

	return mgr, nil
}

// Run instantiates the listeners for a lifecycle manager
func (m *Manager) Run() error {

	// Start consumers
	m.refreshConsumers()

	var subWatcher registry.IWatcher

	// TODO: Implement watch on subscripitons to update active consumers based on watch updates.

	swh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + subscription.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("subscriptions watch handler data", "pairs", pairs)
			m.subWatchData <- &struct{}{}
		},
	}

	logger.Ctx(m.ctx).Infof("setting watch on subscriptions")
	subWatcher, err := m.registry.Watch(m.ctx, &swh)
	if err != nil {
		return err
	}

	leadgrp, gctx := errgroup.WithContext(m.ctx)

	// watch the Subscriptions path for new subscriptions and rebalance
	leadgrp.Go(func() error {
		watchErr := subWatcher.StartWatch()
		close(m.subWatchData)
		return watchErr
	})

	// wait for done channel to be closed and stop watches if received done
	leadgrp.Go(func() error {
		<-gctx.Done()

		if subWatcher != nil {
			subWatcher.StopWatch()
		}

		logger.Ctx(gctx).Info("scheduler group context done")
		return gctx.Err()
	})
	for {
		select {
		case <-m.ctx.Done():
			for h := range m.consumers {
				delete(m.consumers, h)
			}
			return nil
		case val := <-m.subWatchData:
			if val == nil {
				continue
			}
			allSubs, serr := m.subscriptionCore.List(gctx, subscription.Prefix)
			if serr != nil {
				logger.Ctx(gctx).Errorw("error fetching new subscription list", "error", serr)
				return err
			}
			// Filter Push Subscriptions
			var newSubs []*subscription.Model
			for _, sub := range allSubs {
				if !sub.IsPush() {
					newSubs = append(newSubs, sub)
				}
			}

			m.subCache = newSubs
			m.refreshConsumers()
		case cleanupMessage := <-m.cleanupCh:
			logger.Ctx(m.ctx).Infow("lifecyclemanager: got request to cleanup subscriber", "cleanupMessage", cleanupMessage)
			m.CloseConsumer(m.ctx, cleanupMessage.computedHash)
		}
	}
}

func (m *Manager) refreshConsumers() {

	logger.Ctx(m.ctx).Infow("lifecyclemanager: Consumer refresh intiated")
	existingConsumers := make(map[int]subPartition)
	consumersToAdd := make(map[int]subPartition)

	for _, sub := range m.subCache {
		subPart, err := m.subscriptionCore.FetchPartitionsForHash(m.ctx, sub, m.ordinalID)
		if err != nil {
			logger.Ctx(m.ctx).Errorw("lifecyclemanager: Error resolving partitions for subscription", "subscription", sub.Name)
		}
		for _, partition := range subPart {
			computedHash := m.subscriptionCore.FetchSubscriptionHash(m.ctx, sub.Name, partition)
			if _, ok := m.consumers[computedHash]; ok {
				existingConsumers[computedHash] = subPartition{
					subscription: sub,
					partition:    partition,
				}
			} else {
				consumersToAdd[computedHash] = subPartition{
					subscription: sub,
					partition:    partition,
				}
			}
		}
	}

	logger.Ctx(m.ctx).Infow("lifecyclemanager: Consumer update status", "existing", len(existingConsumers), "new", len(consumersToAdd))
	// Gracefully shutdown reassigned consumers
	for h := range m.consumers {
		if _, ok := m.consumers[h]; !ok {
			m.cleanupCh <- cleanupMessage{
				computedHash: h,
			}
		}

	}
	for hash, sp := range consumersToAdd {
		consumer, err := m.CreateConsumer(m.ctx, sp.subscription, sp.partition, hash)
		if err != nil {
			// Proceed without failing since this requires other subscribers to be setup
			logger.Ctx(m.ctx).Errorw("lifecyclemanager: failed to create consumer for subscription-partition assignment", "subscription", sp.subscription.Name, "partition", sp.partition)
		} else {
			logger.Ctx(m.ctx).Infow("lifecyclemanager: Successfully added consumer", "consumerHash", hash, "subscription", sp.subscription.Name, "partition", sp.partition)
			m.consumers[hash] = consumer
		}

	}

}

// CreateConsumer sets up the underlying subscriber and returns the consumer object
func (m *Manager) CreateConsumer(ctx context.Context, sub *subscription.Model, partition int, hash int) (*Consumer, error) {
	subscriberID := uuid.New().String()

	subscriberRequestCh := make(chan *subscriber.PullRequest)
	subscriberAckCh := make(chan *subscriber.AckMessage)
	subscriberModAckCh := make(chan *subscriber.ModAckMessage)

	subscriber, err := m.subscriberCore.NewSubscriber(ctx, subscriberID, sub, 100, 50, 5000, subscriberRequestCh, subscriberAckCh, subscriberModAckCh)
	// subscriber, err := m.subscriberCore.NewOpinionatedSubscriber(ctx, subscriberID, sub, partition, hash, 100, 50, 5000,
	// 	subscriberRequestCh, subscriberAckCh, subscriberModAckCh)
	if err != nil {
		logger.Ctx(ctx).Errorw("lifecyclemanager: failed to create subscriber for subscription-partition", "subscription", sub.Name, "partition", partition)
		return nil, err
	}
	return &Consumer{
		ctx:                    m.ctx,
		computedHash:           hash,
		subscriberID:           subscriberID,
		subscription:           sub,
		subscriberCore:         m.subscriberCore,
		subscriptionSubscriber: subscriber,
	}, nil

}

// GetConsumer fetches the relevant consumer from the memory map.
func (m *Manager) GetConsumer(ctx context.Context, sub string, partition int) (*Consumer, error) {
	computedHash := m.subscriptionCore.FetchSubscriptionHash(ctx, sub, partition)
	consumer := m.consumers[computedHash]
	if consumer == nil {
		return nil, errors.Errorf("lifecyclemanager: No active consumer found")
	}
	return consumer, nil
}

// CloseConsumer ensures that the consumer is greacefully exited.
func (m *Manager) CloseConsumer(ctx context.Context, computedhash int) error {
	con := m.consumers[computedhash]
	if con != nil {
		m.mutex.Lock()
		con.stop()
		delete(m.consumers, computedhash)
		m.mutex.Unlock()
	}

	return nil
}

// cleanupMessage ...
type cleanupMessage struct {
	computedHash int
}

type subPartition struct {
	subscription *subscription.Model
	partition    int
}
