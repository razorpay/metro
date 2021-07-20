package tasks

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/scheduler"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/leaderelection"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// SchedulerTask implements the scheduling of subscriptions over nodes.
// only leader node elected using the leader election process does scheduling
type SchedulerTask struct {
	id               string
	name             string
	ttl              time.Duration
	registry         registry.IRegistry
	nodeCore         node.ICore
	scheduler        scheduler.IScheduler
	topicCore        topic.ICore
	nodeBindingCore  nodebinding.ICore
	subscriptionCore subscription.ICore
	nodeCache        []*node.Model
	subCache         []*subscription.Model
}

// NewSchedulerTask creates SchedulerTask instance
func NewSchedulerTask(id string, registry registry.IRegistry, brokerStore brokerstore.IBrokerStore, options ...Option) (ITask, error) {
	options = append(defaultOptions(), options...)

	nodeCore := node.NewCore(node.NewRepo(registry))
	projectCore := project.NewCore(project.NewRepo(registry))
	topicCore := topic.NewCore(topic.NewRepo(registry), projectCore, brokerStore)
	subscriptionCore := subscription.NewCore(
		subscription.NewRepo(registry),
		projectCore,
		topicCore)

	nodeBindingCore := nodebinding.NewCore(nodebinding.NewRepo(registry))

	scheduler, err := scheduler.New(scheduler.LoadBalance)
	if err != nil {
		return nil, err
	}

	schedulerTask := &SchedulerTask{
		id:               id,
		registry:         registry,
		nodeCore:         nodeCore,
		scheduler:        scheduler,
		topicCore:        topicCore,
		nodeBindingCore:  nodeBindingCore,
		subscriptionCore: subscriptionCore,
		nodeCache:        []*node.Model{},
		subCache:         []*subscription.Model{},
	}

	for _, option := range options {
		option(schedulerTask)
	}

	return schedulerTask, nil
}

func defaultOptions() []Option {
	return []Option{
		WithTTL(30 * time.Second),
		WithName("metro/metro-worker"),
	}
}

// WithTTL defines the TTL for the registry session
func WithTTL(ttl time.Duration) Option {
	return func(task ITask) {
		scheduleManager := task.(*SchedulerTask)
		scheduleManager.ttl = ttl
	}
}

// WithName defines the Name for the registry session creation
func WithName(name string) Option {
	return func(task ITask) {
		scheduleManager := task.(*SchedulerTask)
		scheduleManager.name = name
	}
}

// Run the task
func (sm *SchedulerTask) Run(ctx context.Context) error {
	logger.Ctx(ctx).Infow("starting worker schedule task")

	// Create a registry session
	sessionID, err := sm.registry.Register(ctx, sm.name, sm.ttl)
	if err != nil {
		return err
	}

	// Run the tasks
	taskGroup, gctx := errgroup.WithContext(ctx)

	// Renew session periodically
	taskGroup.Go(func() error {
		return sm.registry.RenewPeriodic(gctx, sessionID, sm.ttl, gctx.Done())
	})

	// Acquire the node path using sessionID
	taskGroup.Go(func() error {
		return sm.acquireNode(gctx, sessionID)
	})

	// Run LeaderElection using sessionID
	taskGroup.Go(func() error {
		return sm.runLeaderElection(gctx, sessionID)
	})

	err = taskGroup.Wait()
	logger.Ctx(ctx).Infow("exiting from worker schedule task", "error", err)
	return err
}

func (sm *SchedulerTask) acquireNode(ctx context.Context, sessionID string) error {
	err := sm.nodeCore.AcquireNode(ctx, &node.Model{
		ID: sm.id,
	}, sessionID)

	return err
}

func (sm *SchedulerTask) runLeaderElection(ctx context.Context, sessionID string) error {
	// Init Leader Election
	candidate, err := leaderelection.New(
		sm.id,
		sessionID,
		leaderelection.Config{
			LockPath: common.GetBasePrefix() + "leader/election",
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) error {
					return sm.lead(ctx)
				},
				OnStoppedLeading: func(ctx context.Context) {
					sm.stepDown(ctx)
				},
			},
		}, sm.registry)

	if err != nil {
		return err
	}

	// Run Leader Election
	return candidate.Run(ctx)
}

func (sm *SchedulerTask) lead(ctx context.Context) error {
	logger.Ctx(ctx).Infof("Node %s elected as new leader", sm.id)

	var (
		nodeWatcher, subWatcher registry.IWatcher
		err                     error
	)

	nodeWatchData := make(chan *struct{})
	subWatchData := make(chan *struct{})

	leadgrp, gctx := errgroup.WithContext(ctx)

	nwh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + node.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
			nodeWatchData <- &struct{}{}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on nodes")
	nodeWatcher, err = sm.registry.Watch(gctx, &nwh)
	if err != nil {
		return err
	}

	swh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + subscription.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("subscriptions watch handler data", "pairs", pairs)
			subWatchData <- &struct{}{}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on subscriptions")
	subWatcher, err = sm.registry.Watch(gctx, &swh)
	if err != nil {
		return err
	}

	// watch for nodes addition/deletion, for any changes a rebalance might be required
	leadgrp.Go(func() error {
		watchErr := nodeWatcher.StartWatch()
		close(nodeWatchData)

		return watchErr
	})

	// watch the Subscriptions path for new subscriptions and rebalance
	leadgrp.Go(func() error {
		watchErr := subWatcher.StartWatch()
		close(subWatchData)
		return watchErr
	})

	// handle node and subscription updates
	leadgrp.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case val := <-subWatchData:
				if val == nil {
					return nil
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
					// just log the error, we want to retry the sub update failures
					logger.Ctx(gctx).Infow("error processing subscription updates", "error", serr)
				}
			case val := <-nodeWatchData:
				if val == nil {
					return nil
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

		logger.Ctx(gctx).Info("leader context returned done")
		return gctx.Err()
	})

	// wait for leader go routines to terminate
	err = leadgrp.Wait()

	if err != nil && err != context.Canceled {
		logger.Ctx(gctx).Errorf("Error in leader group go routines : %s", err.Error())
	}

	return err
}

func (sm *SchedulerTask) stepDown(ctx context.Context) {
	logger.Ctx(ctx).Infof("Node %s stepping down from leader", sm.id)
}

func (sm *SchedulerTask) refreshNodeBindings(ctx context.Context) error {
	// fetch all current node bindings across all nodes
	nodeBindings, err := sm.nodeBindingCore.List(ctx, nodebinding.Prefix)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching new node binding list", "error", err)
		return err
	}

	// Delete any binding where subscription is removed, This needs to be handled before nodes updates
	// as node update will cause subscriptions to be rescheduled on other nodes
	var validBindings []*nodebinding.Model

	// TODO: Optimize O(N*M) to O(N+M)
	for _, nb := range nodeBindings {
		found := false
		for _, sub := range sm.subCache {
			if sub.Name == nb.SubscriptionID {
				found = true
				break
			}
		}

		if !found {
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
			sm.nodeBindingCore.DeleteNodeBinding(ctx, nb)
		} else {
			validBindings = append(validBindings, nb)
		}
	}

	// update the binding list after deletions
	nodeBindings = validBindings

	// Reschedule Bindings which are invalid due to node failures
	for _, nb := range invalidBindings {
		logger.Ctx(ctx).Infow("rescheduling subscription on nodes", "key", nb.SubscriptionID)

		sub, serr := sm.subscriptionCore.Get(ctx, nb.SubscriptionID)
		if serr != nil {
			return serr
		}

		serr = sm.scheduleSubscription(ctx, sub, &nodeBindings)
		if serr != nil {
			return serr
		}
	}

	// Create bindings for new subscriptions
	for _, sub := range sm.subCache {
		found := false
		for _, nb := range nodeBindings {
			if sub.Name == nb.SubscriptionID {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(ctx).Infow("scheduling subscription on nodes", "subscription", sub.Name, "topic", sub.Topic)

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
		return serr
	}

	berr := sm.nodeBindingCore.CreateNodeBinding(ctx, nb)
	if berr != nil {
		return berr
	}

	*nodeBindings = append(*nodeBindings, nb)
	return nil
}
