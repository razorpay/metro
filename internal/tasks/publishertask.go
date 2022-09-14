package tasks

import (
	"context"
	"log"
	"strconv"

	"golang.org/x/sync/errgroup"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// PublisherTask implements the Watcher and maintains a pre-warmup.
type PublisherTask struct {
	id              string
	registry        registry.IRegistry
	topicCore       topic.ICore
	nodeBindingCore nodebinding.ICore
	topicWatchData  chan *struct{}
}

var topicCacheData map[string]bool = make(map[string]bool)

// NewPublisherTask creates PublisherTask instance
func NewPublisherTask(
	id string,
	registry registry.IRegistry,
	topicCore topic.ICore,
	nodeBindingCore nodebinding.ICore,
	options ...Option,
) (ITask, error) {
	publisherTask := &PublisherTask{
		id:              id,
		registry:        registry,
		topicCore:       topicCore,
		nodeBindingCore: nodeBindingCore,
		topicWatchData:  make(chan *struct{}),
	}

	for _, option := range options {
		option(publisherTask)
	}

	return publisherTask, nil
}

// Run the task
func (pu *PublisherTask) Run(ctx context.Context) error {
	logger.Ctx(ctx).Infow("running publisher task", "workerID", pu.id)
	var err error
	var topicWatcher registry.IWatcher

	twh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + topic.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("topic watch handler data", "pairs", pairs)
			pu.topicWatchData <- &struct{}{}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on topics")
	topicWatcher, err = pu.registry.Watch(ctx, &twh)
	if err != nil {
		return err
	}

	leadgrp, gctx := errgroup.WithContext(ctx)

	// watch the Topic path for new topics and rebalance
	leadgrp.Go(func() error {
		watchErr := topicWatcher.StartWatch()
		close(pu.topicWatchData)
		return watchErr
	})

	// handle topic updates
	leadgrp.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case val := <-pu.topicWatchData:
				if val == nil {
					continue
				}
				terr := pu.refreshNodeBindings(gctx)
				if terr != nil {
					logger.Ctx(gctx).Infow("error processing topic updates", "error", terr)
				} else {
					logger.Ctx(gctx).Infow("Node bindings refreshed")
				}
			}
		}
	})

	// wait for done channel to be closed and stop watches if received done
	leadgrp.Go(func() error {
		<-gctx.Done()

		if topicWatcher != nil {
			topicWatcher.StopWatch()
		}

		logger.Ctx(gctx).Info("publisher group context done")
		return gctx.Err()
	})

	// wait for leader go routines to terminate
	err = leadgrp.Wait()

	if err != nil && err != context.Canceled {
		logger.Ctx(gctx).Errorf("Error in leader group go routines : %s", err.Error())
	}

	return err
}

func (pu *PublisherTask) refreshCache(ctx context.Context) error {
	log.Printf("Refreshing cache....")
	topics, terr := pu.topicCore.List(ctx, topic.Prefix)
	if terr != nil {
		logger.Ctx(ctx).Errorw("error fetching topic list", "error", terr)
		return terr
	}

	topicData := make(map[string]bool)
	for _, topic := range topics {
		topicData[topic.Name] = true
	}
	topicCacheData = topicData

	log.Println("Topic Data cache: ", topicCacheData)
	return nil
}

// refreshNodeBindings achieves the following in the order outline:
// 1. Go through node bindings and remove invalid bindings
// 	 (Invalid due to changes in the subscription, node failures, topic updates, etc)
//	  a. Remove bindings that do not conform to the new partition based approach.
//    b. Remove nodebindings for deleted/invalid subscriptions.
//    c. Remove nodebindings impacted by node failures
// 2. Topic changes are inherently covered since subscription validates against topic.
func (pu *PublisherTask) refreshNodeBindings(ctx context.Context) error {
	err := pu.refreshCache(ctx)
	if err != nil {
		logger.Ctx(ctx).Errorw("PublisherTask: Failed to refresh cache for topic/subscription/nodes", "error", err.Error())
	}
	// fetch all current node bindings across all nodes
	nodeBindings, err := pu.nodeBindingCore.List(ctx, nodebinding.Prefix)

	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching new node binding list", "error", err)
		return err
	}
	nodeBindingKeys, err := pu.nodeBindingCore.ListKeys(ctx, nodebinding.Prefix)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching node bindings keys", "error", err)
		return err
	}

	nbKeymap := make(map[string]string)
	for _, v := range nodeBindingKeys {
		nbKeymap[v] = v
	}

	// Get Invalid Node Bindings
	invalidBindings := make(map[string]*nodebinding.Model)

	for _, nb := range nodeBindings {
		// Remove non-partition specific nodebindings.
		invalidKey := nb.DefunctKey()
		if _, ok := nbKeymap[invalidKey]; ok {
			invalidBindings[invalidKey] = nb
			continue
		}
	}

	logger.Ctx(ctx).Infow("publishertask: Resolved invalid bindings to be deleted", "invalidBindings", invalidBindings)

	// Delete invalid node bindings
	for key, nb := range invalidBindings {
		dErr := pu.nodeBindingCore.DeleteNodeBinding(ctx, key, nb)
		if err != nil {
			logger.Ctx(ctx).Errorw("publishertask: failed to delete invalid node binding", "error", dErr.Error(), "subscripiton", nb.SubscriptionID, "partition", nb.Partition, "nodebinding", nb.ID)
		}
	}

	// Fetch the latest node bindings after deletions
	nodeBindings, err = pu.nodeBindingCore.List(ctx, nodebinding.Prefix)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching new node binding list", "error", err)
		return err
	}
	validBindings := make(map[string]*nodebinding.Model)

	for _, nb := range nodeBindings {
		subPart := nb.SubscriptionID + "_" + strconv.Itoa(nb.Partition)
		validBindings[subPart] = nb
	}

	return nil
}

// CheckIfTopicExists is to check if topic exists inside the cache
func CheckIfTopicExists(ctx context.Context, topic string) bool {
	// Get Topic Cache and check in topic exists
	topicData := topicCacheData
	log.Println("TopicData: ", topicData)
	if val, ok := topicData[topic]; ok {
		println("Topic Object with topic name: ", topic, "| Object: ", val)
		return true
	}
	return false
}
