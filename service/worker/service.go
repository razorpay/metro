package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/pkg/leaderelection"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	"golang.org/x/sync/errgroup"
)

// Service for worker
type Service struct {
	ctx       context.Context
	health    *health.Core
	config    *Config
	registry  registry.IRegistry
	candidate *leaderelection.Candidate
	nodeID    string
	doneCh    chan struct{}
	stopCh    chan struct{}
	workgrp   *errgroup.Group
	leadgrp   *errgroup.Group
}

// NewService creates an instance of new worker
func NewService(ctx context.Context, config *Config) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
		nodeID: uuid.New().String(),
		doneCh: make(chan struct{}),
		stopCh: make(chan struct{}),
	}
}

// Start implements all the tasks for worker and waits until one of the task fails
func (c *Service) Start() error {
	// close the done channel when this function returns
	defer close(c.doneCh)

	var (
		err  error
		gctx context.Context
	)

	c.workgrp, gctx = errgroup.WithContext(c.ctx)

	// Init the Registry
	// TODO: move to component init ?
	c.registry, err = registry.NewRegistry(c.ctx, &c.config.Registry)

	if err != nil {
		return err
	}

	// Init Leader Election
	c.candidate, err = leaderelection.New(c.nodeID,
		leaderelection.Config{
			// TODO: read values from config
			Name:          "metro-worker",
			NodePath:      fmt.Sprintf("registry/nodes/%s", c.nodeID),
			LockPath:      "leader/election",
			LeaseDuration: 30 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) error {
					return c.lead(ctx)
				},
				OnStoppedLeading: func() {
					c.stepDown()
				},
			},
		}, c.registry)

	if err != nil {
		return err
	}

	// Run leader election
	c.workgrp.Go(func() error {
		logger.Ctx(gctx).Info("starting leader election")
		return c.candidate.Run(gctx)
	})

	// Watch for the subscription assignment changes
	var watcher registry.IWatcher
	c.workgrp.Go(func() error {
		wh := registry.WatchConfig{
			WatchType: "keyprefix",
			WatchPath: fmt.Sprintf("/registry/nodes/%s/subscriptions", c.nodeID),
			Handler: func(ctx context.Context, pairs []registry.Pair) {
				logger.Ctx(ctx).Infow("node subscriptions", "pairs", pairs)
			},
		}

		watcher, err = c.registry.Watch(gctx, &wh)
		if err != nil {
			return err
		}

		return watcher.StartWatch()
	})

	c.workgrp.Go(func() error {
		var err error
		select {
		case <-gctx.Done():
			err = gctx.Err()
		case <-c.stopCh:
			err = fmt.Errorf("signal received, stopping worker")
		}

		if watcher != nil {
			watcher.StopWatch()
		}

		return err
	})

	err = c.workgrp.Wait()
	logger.Ctx(gctx).Info("push consumer servicer error: %s", err.Error())
	return err
}

// Stop the service
func (c *Service) Stop() error {
	// signal to stop all go routines
	close(c.stopCh)

	// wait until all goroutines are done
	<-c.doneCh

	return nil
}

func (c *Service) lead(ctx context.Context) error {
	logger.Ctx(ctx).Infof("Node %s elected as new leader", c.nodeID)

	var (
		nodeWatcher, subWatcher registry.IWatcher
		err                     error
		gctx                    context.Context
	)
	c.leadgrp, gctx = errgroup.WithContext(ctx)

	nwh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: "/registry/nodes",
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
		},
	}

	nodeWatcher, err = c.registry.Watch(gctx, &nwh)
	if err != nil {
		return err
	}

	swh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: "/registry/subscriptions",
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
		},
	}

	subWatcher, err = c.registry.Watch(gctx, &swh)
	if err != nil {
		return err
	}

	// watch for nodes addition/deletion, for any changes a rebalance might be required
	c.leadgrp.Go(func() error {
		return nodeWatcher.StartWatch()
	})

	// watch the Subscriptions path for new subscriptions and rebalance
	c.leadgrp.Go(func() error {
		return subWatcher.StartWatch()
	})

	// wait for done channel to be closed and stop watches if received done
	c.leadgrp.Go(func() error {
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

	return nil
}

func (c *Service) stepDown() {
	logger.Ctx(c.ctx).Infof("Node %s stepping down from leader", c.nodeID)

	// wait for leader go routines to terminate
	c.leadgrp.Wait()
}
