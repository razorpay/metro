package pushconsumer

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/razorpay/metro/internal/node"

	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/pkg/leaderelection"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	"golang.org/x/sync/errgroup"
)

// Service for push consumer
type Service struct {
	ctx       context.Context
	health    *health.Core
	config    *Config
	registry  registry.IRegistry
	candidate *leaderelection.Candidate
	node      *node.Model
	doneCh    chan struct{}
	stopCh    chan struct{}
	workgrp   *errgroup.Group
	leadgrp   *errgroup.Group
}

// NewService creates an instance of new push consumer service
func NewService(ctx context.Context, config *Config) *Service {
	id := uuid.New().String()
	return &Service{
		ctx:     context.WithValue(ctx, "NodeId", id),
		config:  config,
		doneCh:  make(chan struct{}),
		stopCh:  make(chan struct{}),
		leadgrp: &errgroup.Group{},
		node: &node.Model{
			ID:   id,
			Name: "push-consumer",
		},
	}
}

// Start implements all the tasks for push-consumer and waits until one of the task fails
func (c *Service) Start() error {
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
	c.candidate, err = leaderelection.New(leaderelection.Config{
		// TODO: read values from config
		Name:          "metro-push-consumer",
		Path:          "leader/election",
		LeaseDuration: 30 * time.Second,
		RenewDeadline: 20 * time.Second,
		RetryPeriod:   5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				c.lead(ctx)
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
		return c.candidate.Run(gctx)
	})

	c.workgrp.Go(func() error {
		// Node registration with registry
		nodeCore := node.NewCore(node.NewRepo(c.registry))
		return nodeCore.CreateNode(gctx, c.node)

		// watch for subscription for the node
		nodepath := fmt.Sprintf("/registry/nodes/%s/subscriptions", c.node.ID)
		return c.registry.Watch(
			"keyprefix",
			nodepath,
			func(pairs []registry.Pair) {
				logger.Ctx(gctx).Infow("node subscriptions", "pairs", pairs)
			})
	})

	c.workgrp.Go(func() error {
		<-c.stopCh
		return fmt.Errorf("signal received, stopping push-consumer")
	})

	err = c.workgrp.Wait()
	close(c.doneCh)
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

func (c *Service) lead(ctx context.Context) {
	logger.Ctx(ctx).Infof("Node %s elected as new leader", c.node.ID)

	// watch for nodes addition/deletion, for any changes a rebalance might be required
	c.leadgrp.Go(func() error {
		return c.registry.Watch(
			"keyprefix",
			"/registry/nodes",
			func(pairs []registry.Pair) {
				logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
			})
	})

	// Watch the Subscriptions path for new subscriptions and rebalance
	c.leadgrp.Go(func() error {
		return c.registry.Watch(
			"keyprefix",
			"/registry/subscriptions",
			func(pairs []registry.Pair) {
				logger.Ctx(ctx).Infow("subscriptions watch handler data", "pairs", pairs)
			})
	})

	c.leadgrp.Go(func() error {
		<-ctx.Done()
		logger.Ctx(ctx).Info("leader context returned done")
		return ctx.Err()
	})
}

func (c *Service) stepDown() {
	logger.Ctx(c.ctx).Infof("Node %s stepping down from leader", c.node.ID)

	// wait for leader go routines to terminate
	c.leadgrp.Wait()
}
