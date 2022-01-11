package tasks

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/scheduler"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// SchedulerTask implements the scheduling of subscriptions over nodes.
// only leader node elected using the leader election process does scheduling
type SchedulerTask struct {
	id               string
	registry         registry.IRegistry
	brokerstore      brokerstore.IBrokerStore
	nodeCore         node.ICore
	scheduler        scheduler.IScheduler
	topicCore        topic.ICore
	nodeBindingCore  nodebinding.ICore
	subscriptionCore subscription.ICore
	nodeCache        []*node.Model
	subCache         []*subscription.Model
	nodeWatchData    chan *struct{}
	subWatchData     chan *struct{}
}

// NewSchedulerTask creates SchedulerTask instance
func NewSchedulerTask(
	id string,
	registry registry.IRegistry,
	brokerStore brokerstore.IBrokerStore,
	nodeCore node.ICore,
	topicCore topic.ICore,
	nodeBindingCore nodebinding.ICore,
	subscriptionCore subscription.ICore,
	scheduler scheduler.IScheduler,
	options ...Option,
) (ITask, error) {
	schedulerTask := &SchedulerTask{
		id:               id,
		registry:         registry,
		brokerstore:      brokerStore,
		nodeCore:         nodeCore,
		scheduler:        scheduler,
		topicCore:        topicCore,
		nodeBindingCore:  nodeBindingCore,
		subscriptionCore: subscriptionCore,
		nodeCache:        []*node.Model{},
		subCache:         []*subscription.Model{},
		nodeWatchData:    make(chan *struct{}),
		subWatchData:     make(chan *struct{}),
	}

	for _, option := range options {
		option(schedulerTask)
	}

	return schedulerTask, nil
}

// Run the task
func (sm *SchedulerTask) Run(ctx context.Context) error {
	logger.Ctx(ctx).Infow("running worker scheduler task", "workerID", sm.id)

	var err error
	var nodeWatcher registry.IWatcher
	var subWatcher registry.IWatcher

	nwh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + node.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
			sm.nodeWatchData <- &struct{}{}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on nodes")
	nodeWatcher, err = sm.registry.Watch(ctx, &nwh)
	if err != nil {
		return err
	}

	swh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + subscription.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("subscriptions watch handler data", "pairs", pairs)
			sm.subWatchData <- &struct{}{}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on subscriptions")
	subWatcher, err = sm.registry.Watch(ctx, &swh)
	if err != nil {
		return err
	}

	leadgrp, gctx := errgroup.WithContext(ctx)

	// watch for nodes addition/deletion, for any changes a rebalance might be required
	leadgrp.Go(func() error {
		watchErr := nodeWatcher.StartWatch()
		close(sm.nodeWatchData)
		return watchErr
	})

	// watch the Subscriptions path for new subscriptions and rebalance
	leadgrp.Go(func() error {
		watchErr := subWatcher.StartWatch()
		close(sm.subWatchData)
		return watchErr
	})

	// handle node and subscription updates
	leadgrp.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case val := <-sm.subWatchData:
				if val == nil {
					continue
				}
				allSubs, serr := sm.subscriptionCore.List(gctx, subscription.Prefix)
				if serr != nil {
					logger.Ctx(gctx).Errorw("error fetching new subscription list", "error", serr)
					return err
				}
				// Filter Push Subscriptions
				var newSubs []*subscription.Model
				for _, sub := range allSubs {
					if sub.IsPush() {
						newSubs = append(newSubs, sub)
					}
				}

				sm.subCache = newSubs
				serr = sm.refreshNodeBindings(gctx)
				if serr != nil {
					subNames := make([]string, 0)

					for _, sub := range newSubs {
						subNames = append(subNames, sub.ExtractedSubscriptionName)
					}
					// just log the error, we want to retry the sub update failures
					logger.Ctx(gctx).Errorw("error processing subscription updates", "error", serr, "logFields", map[string]interface{}{
						"subscriptions": subNames,
					})
				}
			case val := <-sm.nodeWatchData:
				if val == nil {
					continue
				}
				nodes, nerr := sm.nodeCore.List(gctx, node.Prefix)
				if nerr != nil {
					logger.Ctx(gctx).Errorw("error fetching new node list", "error", nerr)
					return nerr
				}
				sm.nodeCache = nodes
				nerr = sm.refreshNodeBindings(gctx)
				if nerr != nil {
					logger.Ctx(gctx).Infow("error processing node updates", "error", nerr)
				}

			}
		}
	})

	// wait for done channel to be closed and stop watches if received done
	leadgrp.Go(func() error {
		<-gctx.Done()

		if nodeWatcher != nil {
			nodeWatcher.StopWatch()
		}

		if subWatcher != nil {
			subWatcher.StopWatch()
		}

		logger.Ctx(gctx).Info("scheduler group context done")
		return gctx.Err()
	})

	// wait for leader go routines to terminate
	err = leadgrp.Wait()

	if err != nil && err != context.Canceled {
		logger.Ctx(gctx).Errorf("Error in leader group go routines : %s", err.Error())
	}

	return err
}

func (sm *SchedulerTask) refreshNodeBindings(ctx context.Context) error {
	// fetch all current node bindings across all nodes
	nodeBindings, err := sm.nodeBindingCore.List(ctx, nodebinding.Prefix)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching new node binding list", "error", err)
		return err
	}

	// Delete any binding where subscription is removed or
	// the subscription's current version id doesn't match the node binding's subscription version id.
	// This needs to be handled before nodes updates
	// as node update will cause subscriptions to be rescheduled on other nodes
	var validBindings []*nodebinding.Model

	// TODO: Optimize O(N*M) to O(N+M)
	for _, nb := range nodeBindings {
		found := false
		for _, sub := range sm.subCache {
			subVersion := sub.GetVersion()
			if sub.Name == nb.SubscriptionID {
				if nb.SubscriptionVersion == subVersion {
					logger.Ctx(ctx).Infow("schedulertask: existing subscription stream found", "subscription", nb.SubscriptionID, "topic", sub.ExtractedTopicName)
					found = true
					break
				}
				logger.Ctx(ctx).Infow("subscription updated, stale node binding will be deleted",
					"subscription", sub.Name, "stale version", nb.SubscriptionVersion, "new version", subVersion)
			}
		}

		if !found {
			logger.Ctx(ctx).Infow("schedulertask: unable to find nodebinding for subscription", "subscription", nb.SubscriptionID)
			sm.nodeBindingCore.DeleteNodeBinding(ctx, nb)
		} else {
			validBindings = append(validBindings, nb)
		}
	}
	// update the binding list after deletions
	nodeBindings = validBindings

	// Reschedule any binding where node is removed
	validBindings = []*nodebinding.Model{}
	var invalidBindings []*nodebinding.Model
	var invalidBindingNames []string
	var validBindingNames []string

	// TODO: Optimize O(N*M) to O(N+M)
	for _, nb := range nodeBindings {
		found := false
		for _, node := range sm.nodeCache {
			if node.ID == nb.NodeID {
				found = true
				break
			}
		}

		if !found {
			invalidBindings = append(invalidBindings, nb)
			invalidBindingNames = append(invalidBindingNames, nb.SubscriptionID)
			sm.nodeBindingCore.DeleteNodeBinding(ctx, nb)
		} else {
			validBindings = append(validBindings, nb)
			validBindingNames = append(validBindingNames, nb.SubscriptionID)
		}
	}

	logger.Ctx(ctx).Infow("schedulertask:  binding validation completed", "invalidBindings", invalidBindingNames, "validBindings", validBindingNames)
	// update the binding list after deletions
	nodeBindings = validBindings

	// Reschedule Bindings which are invalid due to node failures
	for _, nb := range invalidBindings {
		logger.Ctx(ctx).Infow("schedulertask: rescheduling subscription on nodes", "key", nb.SubscriptionID, "subscription", nb.SubscriptionID)

		sub, serr := sm.subscriptionCore.Get(ctx, nb.SubscriptionID)
		if serr != nil {
			return serr
		}

		serr = sm.scheduleSubscription(ctx, sub, &nodeBindings)
		if serr != nil {
			logger.Ctx(ctx).Errorw("schedulertask: failed to update invalid nodebindings", "err", serr, "subscription", sub.ExtractedSubscriptionName, "topic", sub.ExtractedTopicName)
			return serr
		}
	}

	// Create bindings for new subscriptions
	for _, sub := range sm.subCache {

		found := false
		subVersion := sub.GetVersion()
		for _, nb := range nodeBindings {
			if sub.Name == nb.SubscriptionID && nb.SubscriptionVersion == subVersion {
				found = true
				break
			}
		}
		logger.Ctx(ctx).Infow("schedulertask: evaluating nodebinding for subscripiton", "subscription", sub.Name, "topic", sub.ExtractedTopicName, "found", found)
		if !found {
			logger.Ctx(ctx).Infow("scheduling subscription on nodes",
				"subscription", sub.Name, "topic", sub.Topic, "subscription version", subVersion)

			topicM, terr := sm.topicCore.Get(ctx, sub.Topic)
			if terr != nil {
				return terr
			}

			for i := 0; i < topicM.NumPartitions; i++ {
				serr := sm.scheduleSubscription(ctx, sub, &nodeBindings)
				if serr != nil {
					return serr
				}
			}
		}
	}
	return nil
}

func (sm *SchedulerTask) scheduleSubscription(ctx context.Context, sub *subscription.Model, nodeBindings *[]*nodebinding.Model) error {
	nb, serr := sm.scheduler.Schedule(sub, *nodeBindings, sm.nodeCache)
	if serr != nil {
		logger.Ctx(ctx).Errorw("scheduler: failed to schedule subscription", "err", serr.Error(), "logFields", map[string]interface{}{
			"subscription": sub.Name,
			"topic":        sub.Topic,
		})
		return serr
	}

	berr := sm.nodeBindingCore.CreateNodeBinding(ctx, nb)
	if berr != nil {
		logger.Ctx(ctx).Errorw("scheduler: failed to create nodebinding", "err", serr.Error(), "logFields", map[string]interface{}{
			"subscription": sub.Name,
			"topic":        sub.Topic,
		})
		return berr
	}

	*nodeBindings = append(*nodeBindings, nb)
	return nil
}
