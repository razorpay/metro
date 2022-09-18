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

	twh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + topic.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("topic watch handler data", "pairs", pairs)
			pu.topicWatchData <- &struct{}{}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on topics..")
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
				if err != nil {
				}
				if terr != nil {
					logger.Ctx(gctx).Errorw("PublisherTask: Failed to refresh cache for topic/subscription/nodes", "error", err.Error())
				} else {
					logger.Ctx(gctx).Infow("Topic Cache refreshed")
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

	return nil
}

// CheckIfTopicExists is to check if topic exists inside the cache
func CheckIfTopicExists(ctx context.Context, topic string) bool {
	// Get Topic Cache and check in topic exists
	topicData := topicCacheData
	if _, ok := topicData[topic]; ok {
		// println("Topic Object with topic name: ", topic, "| Object: ", val)
		return true
	}
	return false
}

// SetCacheData is to set the Cache Map
func SetCacheData(data map[string]bool) bool {
	topicCacheData = data

	return true
}
