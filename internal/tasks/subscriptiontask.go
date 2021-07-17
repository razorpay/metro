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
	"github.com/razorpay/metro/internal/stream"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// SubscriptionTask runs the assigned subscriptions to the node
type SubscriptionTask struct {
	id               string
	registry         registry.IRegistry
	brokerStore      brokerstore.IBrokerStore
	subscriptionCore subscription.ICore
	nodeBindingCore  nodebinding.ICore
	subscriber       subscriber.ICore
	watcher          registry.IWatcher
	nodebindingCache []*nodebinding.Model
	watchCh          chan []registry.Pair
	pushHandlers     sync.Map
	httpConfig       *stream.HTTPClientConfig
	doneCh           chan struct{}
}

// NewSubscriptionTask creates SubscriptionTask instance
func NewSubscriptionTask(
	id string,
	reg registry.IRegistry,
	brokerStore brokerstore.IBrokerStore,
	subscriptionCore subscription.ICore,
	nodebindingCore nodebinding.ICore,
	subscriberCore subscriber.ICore,
	options ...Option,
) (ITask, error) {
	subscriptionTask := &SubscriptionTask{
		id:               id,
		registry:         reg,
		brokerStore:      brokerStore,
		subscriptionCore: subscriptionCore,
		nodeBindingCore:  nodebindingCore,
		subscriber:       subscriberCore,
		doneCh:           make(chan struct{}),
		watchCh:          make(chan []registry.Pair),
	}

	for _, option := range options {
		option(subscriptionTask)
	}

	return subscriptionTask, nil
}

// WithHTTPConfig defines the httpClient config for wehbooks http client
func WithHTTPConfig(config *stream.HTTPClientConfig) Option {
	return func(task ITask) {
		subscriptionTask := task.(*SubscriptionTask)
		subscriptionTask.httpConfig = config
	}
}

// Run the SubscriptionTask process
func (sm *SubscriptionTask) Run(ctx context.Context) error {
	logger.Ctx(ctx).Infow("starting worker subscription task")
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

	err = taskGroup.Wait()
	logger.Ctx(ctx).Infow("exiting from worker subscription task", "error", err)
	return err
}

func (sm *SubscriptionTask) createNodeBindingWatch(ctx context.Context) error {
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

func (sm *SubscriptionTask) startNodeBindingWatch(ctx context.Context) error {
	// stop watch when context is Done
	go func() {
		<-ctx.Done()

		logger.Ctx(ctx).Infow("stopping node subscriptions watch")
		sm.watcher.StopWatch()
	}()

	logger.Ctx(ctx).Infow("starting node subscriptions watch")
	err := sm.watcher.StartWatch()

	// close the node binding data channel on watch terminationation, this will ensure that no further watch
	// handler call will be triggered and its safe to close the channel
	logger.Ctx(ctx).Infow("watch terminated, closing the node binding data channel")
	close(sm.watchCh)

	return err
}

func (sm *SubscriptionTask) handleWatchUpdates(ctx context.Context) error {
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
				continue
			}

			err := sm.handleNodeBindingUpdates(ctx, pairs)
			if err != nil {
				logger.Ctx(ctx).Infow("error processing nodebinding updates", "error", err)
				return err
			}
		}
	}
}

func (sm *SubscriptionTask) handleNodeBindingUpdates(ctx context.Context, newBindingPairs []registry.Pair) error {
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

func (sm *SubscriptionTask) stopPushHandlers(ctx context.Context) {
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
				logger.Ctx(ctx).Errorw("error stopping stream handler", "error", err)
				return
			}
			logger.Ctx(ctx).Infow("successfully stopped stream handler")
		}(ps, &wg)
		return true
	})
	wg.Wait()
}
