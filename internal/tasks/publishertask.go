package tasks

import (
	"context"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"

	"golang.org/x/sync/errgroup"
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
var sem = make(chan int, 1)

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
	var err error
	var topicWatcher registry.IWatcher
	pu.refreshCache(ctx)

	twh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + topic.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("PublisherTask: topic watch handler data", "pairs", pairs)
			pu.topicWatchData <- &struct{}{}
		},
	}

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
				terr := pu.refreshCache(ctx)
				if terr != nil {
					logger.Ctx(gctx).Errorw("PublisherTask: Failed to refresh cache for topic/subscription/nodes", "error", err.Error())
				} else {
					logger.Ctx(gctx).Infow("PublisherTask: Topic Cache refreshed")
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

		return gctx.Err()
	})

	// wait for leader go routines to terminate
	err = leadgrp.Wait()

	if err != nil && err != context.Canceled {
		logger.Ctx(gctx).Errorw("PublisherTask: Error in leader group go routines : %s", err.Error())
	}

	return err
}

// refreshCache is to refresh Topic Data Cache
func (pu *PublisherTask) refreshCache(ctx context.Context) error {
	topics, terr := pu.topicCore.List(ctx, topic.Prefix)
	if terr != nil {
		logger.Ctx(ctx).Errorw("PublisherTask: error fetching topic list", "error", terr)
		return terr
	}

	topicData := make(map[string]bool)
	for _, topic := range topics {
		topicData[topic.Name] = true
	}
	sem <- 1
	go func() {
		topicCacheData = topicData
		<-sem
	}()

	return nil
}

// CheckIfTopicExists is to check if topic exists inside the cache
func CheckIfTopicExists(ctx context.Context, topic string) bool {
	// Get Topic Cache and check in topic exists
	topicData := topicCacheData
	if _, ok := topicData[topic]; ok {
		return true
	}
	return false
}
