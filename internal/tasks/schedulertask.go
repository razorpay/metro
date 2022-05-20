package tasks

import (
	"context"
	"strconv"

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
	nodeCache        map[string]*node.Model
	subCache         map[string]*subscription.Model
	topicCache       map[string]*topic.Model
	nodeWatchData    chan *struct{}
	subWatchData     chan *struct{}
	topicWatchData   chan *struct{}
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
		nodeCache:        make(map[string]*node.Model),
		subCache:         make(map[string]*subscription.Model),
		topicCache:       make(map[string]*topic.Model),
		nodeWatchData:    make(chan *struct{}),
		subWatchData:     make(chan *struct{}),
		topicWatchData:   make(chan *struct{}),
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
	var topicWatcher registry.IWatcher

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

	twh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + topic.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("topic watch handler data", "pairs", pairs)
			sm.nodeWatchData <- &struct{}{}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on topics")
	topicWatcher, err = sm.registry.Watch(ctx, &twh)
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

	// watch the Subscriptions path for new subscriptions and rebalance
	leadgrp.Go(func() error {
		watchErr := topicWatcher.StartWatch()
		close(sm.topicWatchData)
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

				serr := sm.refreshNodeBindings(gctx)
				if serr != nil {
					subNames := make([]string, 0)

					for _, sub := range sm.subCache {
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
				nerr := sm.refreshNodeBindings(gctx)
				if nerr != nil {
					logger.Ctx(gctx).Infow("error processing node updates", "error", nerr)
				}

			case val := <-sm.topicWatchData:
				if val == nil {
					continue
				}
				terr := sm.refreshNodeBindings(gctx)
				if terr != nil {
					logger.Ctx(gctx).Infow("error processing topic updates", "error", terr)
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

		if topicWatcher != nil {
			topicWatcher.StopWatch()
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

func (sm *SchedulerTask) refreshCache(ctx context.Context) error {
	topics, terr := sm.topicCore.List(ctx, topic.Prefix)
	if terr != nil {
		logger.Ctx(ctx).Errorw("error fetching topic list", "error", terr)
		return terr
	}

	topicData := make(map[string]*topic.Model)
	for _, topic := range topics {
		topicData[topic.Name] = topic
	}
	sm.topicCache = topicData

	nodes, nerr := sm.nodeCore.List(ctx, node.Prefix)
	if nerr != nil {
		logger.Ctx(ctx).Errorw("error fetching new node list", "error", nerr)
		return nerr
	}

	nodeData := make(map[string]*node.Model)
	for _, node := range nodes {
		nodeData[node.ID] = node
	}
	sm.nodeCache = nodeData

	allSubs, serr := sm.subscriptionCore.List(ctx, subscription.Prefix)
	if serr != nil {
		logger.Ctx(ctx).Errorw("error fetching new subscription list", "error", serr)
		return serr
	}
	// Filter Push Subscriptions
	newSubs := make(map[string]*subscription.Model)
	for _, sub := range allSubs {
		if sub.IsPush() {
			newSubs[sub.Name] = sub
		}
	}

	sm.subCache = newSubs
	return nil
}

// refreshNodeBindings achieves the following in the order outline:
// 1. Go through node bindings and remove invalid bindings
// 	 (Invalid due to changes in the subscription, node failures, topic updates, etc)
//	  a. Remove bindings that do not conform to the new partition based approach.
//    b. Remove nodebindings for deleted/invalid subscriptions.
//    c. Remove nodebindings impacted by node failures
// 2. Evaluate subscriptions and schedule any missing subscription/partition combos to nodes available.
// 3. Topic changes are inherently covered since subscription validates against topic.
func (sm *SchedulerTask) refreshNodeBindings(ctx context.Context) error {
	err := sm.refreshCache(ctx)
	if err != nil {
		logger.Ctx(ctx).Errorw("schedulertask: Filed to refresh cache for topic/subscripiton/nodes", "error", err.Error())
	}
	// fetch all current node bindings across all nodes
	nodeBindings, err := sm.nodeBindingCore.List(ctx, nodebinding.Prefix)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching new node binding list", "error", err)
		return err
	}
	nodeBindingKeys, err := sm.nodeBindingCore.ListKeys(ctx, nodebinding.Prefix)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching new node binding list", "error", err)
		return err
	}

	nbKeymap := make(map[string]string)
	for _, v := range nodeBindingKeys {
		nbKeymap[v] = v
	}

	// Delete any binding where subscription is removed or
	// the subscription's current version id doesn't match the node binding's subscription version id.
	// This needs to be handled before nodes updates
	// as node update will cause subscriptions to be rescheduled on other nodes
	var invalidBindings []*nodebinding.Model

	for _, nb := range nodeBindings {

		// Remove non-partition specific nodebindings.
		invalidKey := nb.DefunctKey()
		if _, ok := nbKeymap[invalidKey]; ok {
			invalidBindings = append(invalidBindings, nb)
			continue
		}

		// Remove bindings for nodes that no longer exist.
		if _, ok := sm.nodeCache[nb.NodeID]; !ok {
			invalidBindings = append(invalidBindings, nb)
			continue
		}

		// Remove bindings for deleted subscriptions
		if _, ok := sm.subCache[nb.SubscriptionID]; !ok {
			invalidBindings = append(invalidBindings, nb)
			continue
		}

		sub := sm.subCache[nb.SubscriptionID]
		subVersion := sub.GetVersion()

		if nb.SubscriptionVersion == subVersion {
			logger.Ctx(ctx).Infow("schedulertask: existing subscription stream found", "subscription", nb.SubscriptionID, "topic", sub.ExtractedTopicName, "partition", nb.Partition)
		} else {
			// Remove bindings for subscriptions that have changed.
			invalidBindings = append(invalidBindings, nb)
			logger.Ctx(ctx).Infow("subscription updated, stale node bindings will be deleted",
				"subscription", sub.Name, "stale version", nb.SubscriptionVersion, "new version", subVersion)
		}

	}

	logger.Ctx(ctx).Infow("schedulertask: Resolved invalid bindings to be deleted", "invalidBindings", invalidBindings)

	// Delete bindings which are invalid due to node failures/subscription version changes/subscription deletions
	for _, nb := range invalidBindings {
		err := sm.nodeBindingCore.DeleteNodeBinding(ctx, nb)
		if err != nil {
			logger.Ctx(ctx).Errorw("schedulertask: failed to delete invalid node binding", "subscripiton", nb.SubscriptionID, "partition", nb.Partition, "nodebinding", nb.ID)
		}
	}

	// Fetch the latest node bindings after deletions and schedule missing bindings
	nodeBindings, err = sm.nodeBindingCore.List(ctx, nodebinding.Prefix)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching new node binding list", "error", err)
		return err
	}
	validBindings := make(map[string]*nodebinding.Model)

	for _, nb := range nodeBindings {
		subPart := nb.SubscriptionID + "_" + strconv.Itoa(nb.Partition)
		validBindings[subPart] = nb
	}

	for _, sub := range sm.subCache {
		//Fetch topic and see if all partitions are assigned.
		// If not assign missing ones
		topic := sm.topicCache[sub.Topic]

		for i := 0; i < topic.NumPartitions; i++ {
			subPart := sub.Name + "_" + strconv.Itoa(i)
			if _, ok := validBindings[subPart]; !ok {
				logger.Ctx(ctx).Infow("schedulertask: assigning nodebinding for subscription/partition combo", "topic", sub.Topic, "subscription", sub.Name, "partition", i)
				err := sm.scheduleSubscription(ctx, sub, &nodeBindings, i)
				if err != nil {
					//TODO: Track status here and re-assign if possible
					logger.Ctx(ctx).Errorw("schedulertask: scheduling nodebinding for missing subscription-partition combo", "topic", sub.ExtractedTopicName, "subscription", sub.Name, "partition", i)
				}
			}
		}
	}

	return nil
}

func (sm *SchedulerTask) scheduleSubscription(ctx context.Context, sub *subscription.Model, nodeBindings *[]*nodebinding.Model, partition int) error {
	nb, serr := sm.scheduler.Schedule(sub, partition, *nodeBindings, sm.nodeCache)
	if serr != nil {
		logger.Ctx(ctx).Errorw("scheduler: failed to schedule subscription", "err", serr.Error(), "logFields", map[string]interface{}{
			"subscription": sub.Name,
			"topic":        sub.Topic,
			"partition":    partition,
		})
		return serr
	}

	berr := sm.nodeBindingCore.CreateNodeBinding(ctx, nb)
	if berr != nil {
		logger.Ctx(ctx).Errorw("scheduler: failed to create nodebinding", "err", serr.Error(), "logFields", map[string]interface{}{
			"subscription": sub.Name,
			"topic":        sub.Topic,
			"partition":    partition,
		})
		return berr
	}

	*nodeBindings = append(*nodeBindings, nb)
	logger.Ctx(ctx).Infow("schedulertask: successfully assigned nodebinding for subscription/partition combo", "topic", sub.Topic, "subscription", sub.Name, "partition", partition)
	return nil
}
