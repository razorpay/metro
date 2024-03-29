package tasks

import (
	"context"
	"sync"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"

	"golang.org/x/sync/errgroup"
)

// PublisherTask implements the Watcher and maintains a pre-warmup.
type PublisherTask struct {
	id             string
	registry       registry.IRegistry
	topicCore      topic.ICore
	topicWatchData chan *struct{}
}

// NOTE: Only read queries to be written from Public methods on TopicCacheData

// TopicCacheData is declared Global to keep it instance agnostic
var TopicCacheData map[string]bool = make(map[string]bool)

var mutex sync.Mutex

// NewPublisherTask creates PublisherTask instance
func NewPublisherTask(
	id string,
	registry registry.IRegistry,
	topicCore topic.ICore,
	options ...Option,
) (ITask, error) {

	publisherTask := &PublisherTask{
		id:             id,
		registry:       registry,
		topicCore:      topicCore,
		topicWatchData: make(chan *struct{}),
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

	// Refresh Cache to Buildup cache in beginning
	err = pu.refreshCache(ctx)
	if err != nil {
		return err
	}

	twh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + topic.Prefix,
		Handler: func(
			ctx context.Context,
			pairs []registry.Pair) {
			pu.topicWatchData <- &struct{}{}
		},
	}

	topicWatcher, err = pu.registry.Watch(
		ctx,
		&twh)
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
					logger.Ctx(gctx).Errorw("PublisherTask: Failed to refresh cache for topic", "error", err.Error())
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
	// Fetch Topic List for cache buildup
	topics, terr := pu.topicCore.List(
		ctx,
		topic.Prefix)

	if terr != nil {
		logger.Ctx(ctx).Errorw(
			"PublisherTask: error fetching topic list",
			"error",
			terr)

		return terr
	}

	topicData := make(map[string]bool)
	for _, topic := range topics {
		topicData[topic.Name] = true
	}

	TopicCacheData = topicData
	UpdateTopicCache(ctx, topicData, false)

	return nil
}

// CheckIfTopicExists is to check if topic exists inside the cache
func CheckIfTopicExists(ctx context.Context, topic string) bool {
	// Get Topic Cache and check in topic exists
	if _, ok := TopicCacheData[topic]; ok {

		return true
	}

	return false
}

// UpdateTopicCache is to update the Topic Data cache
func UpdateTopicCache(ctx context.Context, topicMap map[string]bool, specificTopicBool bool) {
	// specificTopicBool : Check to update a specific Topic out of the Topic Data cache which
	// 						was not present in the cache and had to go to the fallback mechanism
	mutex.Lock()
	go func() {
		if specificTopicBool {
			logger.Ctx(ctx).Info("PublisherTask: Missed topic cache update", "topicMap", topicMap)
			for k, v := range topicMap {
				TopicCacheData[k] = v
			}
		} else {
			logger.Ctx(ctx).Info("PublisherTask: overall topic cache update", "topicMap", topicMap)
			TopicCacheData = topicMap
		}
		mutex.Unlock()
	}()
}
