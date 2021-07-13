package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/stream"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// SubscriptionManager runs the assigned subscriptions to the node
type SubscriptionManager struct {
	id               string
	subscriptionCore subscription.ICore
	nodeBindingCore  nodebinding.ICore
	subscriber       subscriber.ICore
	registry         registry.IRegistry
	watcher          registry.IWatcher
	nodebindingCache []*nodebinding.Model
	watchCh          chan []registry.Pair
	pushHandlers     sync.Map
	httpConfig       *stream.HTTPClientConfig
	doneCh           chan struct{}
}

// NewSubscriptionManager creates SubscriptionManager instance
func NewSubscriptionManager(id string, reg registry.IRegistry, brokerStore brokerstore.IBrokerStore, options ...Option) (IManager, error) {
	projectCore := project.NewCore(project.NewRepo(reg))

	topicCore := topic.NewCore(topic.NewRepo(reg), projectCore, brokerStore)

	subscriptionCore := subscription.NewCore(
		subscription.NewRepo(reg),
		projectCore,
		topicCore)

	nodeBindingCore := nodebinding.NewCore(nodebinding.NewRepo(reg))

	subscriberCore := subscriber.NewCore(brokerStore, subscriptionCore)

	subscriptionManager := &SubscriptionManager{
		id:               id,
		registry:         reg,
		subscriptionCore: subscriptionCore,
		nodeBindingCore:  nodeBindingCore,
		subscriber:       subscriberCore,
		doneCh:           make(chan struct{}),
		watchCh:          make(chan []registry.Pair),
	}

	for _, option := range options {
		option(subscriptionManager)
	}

	return subscriptionManager, nil
}

// WithHTTPConfig defines the httpClient config for wehbooks http client
func WithHTTPConfig(config *stream.HTTPClientConfig) Option {
	return func(manager IManager) {
		subscriptionManager := manager.(*SubscriptionManager)
		subscriptionManager.httpConfig = config
	}
}

// Run the SubscriptionManager process
func (sm *SubscriptionManager) Run(ctx context.Context) error {
	logger.Ctx(ctx).Infow("starting subscription manager")
	defer close(sm.doneCh)

	err := sm.createNodeBindingWatch(ctx)
	if err != nil {
		return err
	}

	// Run the tasks
	taskGroup, gctx := errgroup.WithContext(ctx)

	// Watch for the subscription assignment changes
	taskGroup.Go(func() error {
		return sm.startNodeBindingWatch(gctx)
	})

	// Watch for the subscription assignment changes
	taskGroup.Go(func() error {
		return sm.handleWatchUpdates(gctx)
	})

	return taskGroup.Wait()
}

// Stop the SubscriptionManager process
func (sm *SubscriptionManager) stop(ctx context.Context) {
	logger.Ctx(ctx).Infow("stopping subscription manager")

	if sm.watcher != nil {
		logger.Ctx(ctx).Infow("stopping the node subscription watch")
		sm.watcher.StopWatch()
	}

	sm.stopPushHandlers(ctx)

	// wait for start to return
	<-sm.doneCh
}

func (sm *SubscriptionManager) createNodeBindingWatch(ctx context.Context) error {
	prefix := fmt.Sprintf(common.GetBasePrefix()+nodebinding.Prefix+"%s/", sm.id)
	logger.Ctx(ctx).Infow("setting up node subscriptions watch", "prefix", prefix)

	wh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("node subscriptions", "pairs", pairs)
			sm.watchCh <- pairs
		},
	}

	var err error
	sm.watcher, err = sm.registry.Watch(ctx, &wh)
	return err
}

func (sm *SubscriptionManager) startNodeBindingWatch(ctx context.Context) error {
	// stop watch when context is Done
	go func() {
		<-ctx.Done()

		logger.Ctx(ctx).Infow("stopping node subscriptions watch")
		sm.watcher.StopWatch()
	}()


	logger.Ctx(ctx).Infow("starting node subscriptions watch")
	err := sm.watcher.StartWatch()

	// close the node binding data channel on watch terminations
	logger.Ctx(ctx).Infow("watch terminated, closing the node binding data channel")
	close(sm.watchCh)

	return err
}

func (sm *SubscriptionManager) handleWatchUpdates(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Infow("nodebinding handler routine exiting as group context done")
			sm.stopPushHandlers(ctx)
			return ctx.Err()
		case pairs := <-sm.watchCh:
			logger.Ctx(ctx).Infow("received data from node bindings channel", "pairs", pairs)

			// pairs can be be nil if it returns because of closing of channel
			if pairs == nil {
				return nil
			}

			err := sm.handleNodeBindingUpdates(ctx, pairs)
			if err != nil {
				logger.Ctx(ctx).Infow("error processing nodebinding updates", "error", err)
				return err
			}
		}
	}
}

func (sm *SubscriptionManager) handleNodeBindingUpdates(ctx context.Context, newBindingPairs []registry.Pair) error {
	oldBindings := sm.nodebindingCache
	var newBindings []*nodebinding.Model

	for _, pair := range newBindingPairs {
		nb := nodebinding.Model{}
		err := json.Unmarshal(pair.Value, &nb)
		if err != nil {
			return err
		}
		newBindings = append(newBindings, &nb)
	}

	for _, old := range oldBindings {
		oldKey := old.Key()
		found := false
		for _, newBinding := range newBindings {
			if oldKey == newBinding.Key() {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(ctx).Infow("binding removed", "key", oldKey)
			if handler, ok := sm.pushHandlers.Load(oldKey); ok && handler != nil {
				logger.Ctx(ctx).Infow("handler found, calling stop", "key", oldKey)
				go func(ctx context.Context) {
					err := handler.(*stream.PushStream).Stop()
					if err == nil {
						logger.Ctx(ctx).Infow("handler stopped", "key", oldKey)
					}
				}(ctx)
				sm.pushHandlers.Delete(oldKey)
			}
		}
	}

	for _, newBinding := range newBindings {
		found := false
		for _, old := range oldBindings {
			if old.Key() == newBinding.Key() {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(ctx).Infow("binding added", "key", newBinding.Key())
			handler := stream.NewPushStream(ctx, newBinding.ID, newBinding.SubscriptionID, sm.subscriptionCore, sm.subscriber, sm.httpConfig)

			// run the stream in a separate go routine, this go routine is not part of the worker error group
			// as the worker should continue to run if a single subscription stream exists with error
			go func(ctx context.Context) {
				err := handler.Start()
				if err != nil {
					logger.Ctx(ctx).Errorw("[worker]: push stream handler exited",
						"subscription", newBinding.SubscriptionID,
						"error", err.Error())
				}
			}(ctx)

			// store the handler
			sm.pushHandlers.Store(newBinding.Key(), handler)
		}
	}

	sm.nodebindingCache = newBindings
	return nil
}

func (sm *SubscriptionManager) stopPushHandlers(ctx context.Context) {
	// stop all push stream handlers
	logger.Ctx(ctx).Infow("metro stop: stopping all push handlers")
	wg := sync.WaitGroup{}
	sm.pushHandlers.Range(func(_, handler interface{}) bool {
		wg.Add(1)
		ps := handler.(*stream.PushStream)
		go func(ps *stream.PushStream, wg *sync.WaitGroup) {
			defer wg.Done()

			err := ps.Stop()
			if err != nil {
				logger.Ctx(ctx).Infow("error stopping stream handler", "error", err)
			}
		}(ps, &wg)
		return true
	})
	wg.Wait()
}
