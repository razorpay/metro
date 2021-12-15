package consumer

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
)

// ILifecycle interface defines lifecycle manager methods
type ILifecycle interface {
	GetConsumer(ctx context.Context, sub string, partition int) (*Consumer, error)
	CloseConsumer(ctx context.Context, computedHash int) error
	Run()
}

// Manager ...
type Manager struct {
	consumers        map[int]*Consumer
	subscriptionCore subscription.ICore
	cleanupCh        chan cleanupMessage
	replicas         int
	ordinalID        int
	bs               brokerstore.IBrokerStore
	mutex            *sync.Mutex
	ctx              context.Context
}

// NewLifecycleManager ...
func NewLifecycleManager(ctx context.Context, replicas int, ordinalID int, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore, bs brokerstore.IBrokerStore) (ILifecycle, error) {
	mgr := &Manager{
		consumers:        make(map[int]*Consumer),
		subscriptionCore: subscriptionCore,
		bs:               bs,
		replicas:         replicas,
		ordinalID:        ordinalID,
		mutex:            &sync.Mutex{},
		ctx:              ctx,
	}

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

	for _, sub := range pullSubs {
		subPartitions, err := subscriptionCore.FetchPartitionsForHash(ctx, sub, ordinalID)
		if err != nil {
			logger.Ctx(ctx).Errorw("Error resolving partitions for subscription", "subscription", sub.Name)
		}
		for _, partition := range subPartitions {
			subscriberID := uuid.New().String()

			var (
				// init these channels and pass to subscriber
				// the lifecycle of these channels should be maintain by the user
				subscriberRequestCh = make(chan *subscriber.PullRequest)
				subscriberAckCh     = make(chan *subscriber.AckMessage)
				subscriberModAckCh  = make(chan *subscriber.ModAckMessage)
			)
			computedHash := subscriptionCore.FetchSubscriptionHash(ctx, sub.Name, partition)
			subscriber, err := subscriberCore.NewOpinionatedSubscriber(ctx, subscriberID, sub, partition, computedHash, 100, 50, 5000,
				subscriberRequestCh, subscriberAckCh, subscriberModAckCh)
			if err != nil {
				logger.Ctx(ctx).Errorw("lifecyclemanager: failed to create subscriber for subscription-partition", "subscription", sub.Name, "partition", partition)
				// Proceed without failing since this requires other subscribers to be setup
			} else {
				mgr.consumers[computedHash] = &Consumer{
					ctx:                    ctx,
					computedHash:           computedHash,
					subscriberID:           subscriberID,
					subscription:           sub,
					subscriberCore:         subscriberCore,
					subscriptionSubscriber: subscriber,
				}
			}
		}

	}

	// TODO: Implement watch on subscripitons to update active consumers based on watch updates.

	// swh := registry.WatchConfig{
	// 	WatchType: "keyprefix",
	// 	WatchPath: common.GetBasePrefix() + subscription.Prefix,
	// 	Handler: func(ctx context.Context, pairs []registry.Pair) {
	// 		logger.Ctx(ctx).Infow("subscriptions watch handler data", "pairs", pairs)
	// 		sm.subWatchData <- &struct{}{}
	// 	},
	// }

	// logger.Ctx(ctx).Infof("setting watch on subscriptions")
	// subWatcher, err = sm.registry.Watch(ctx, &swh)
	// if err != nil {
	// }

	return mgr, nil
}

// Run instantiates the listeners for a lifecycle manager
func (m *Manager) Run() {

	for {
		select {
		case <-m.ctx.Done():
			for _, con := range m.consumers {
				con.stop()
			}
			return
		case cleanupMessage := <-m.cleanupCh:
			logger.Ctx(m.ctx).Infow("manager: got request to cleanup subscriber", "cleanupMessage", cleanupMessage)
			m.mutex.Lock()
			//Implement cleanup here
			m.mutex.Unlock()
		}
	}
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
	subscriberID string
	subscription string
}
