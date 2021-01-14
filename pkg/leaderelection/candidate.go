package leaderelection

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// New creates a instance of LeaderElector from a LeaderElection Config
func New(id string, c Config, registry registry.IRegistry) (*Candidate, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	le := Candidate{
		ID:       id,
		config:   c,
		registry: registry,
		nodeID:   "",
		leader:   false,
		errCh:    make(chan error),
	}

	return &le, nil
}

// Candidate is a leader election candidate.
type Candidate struct {
	// ID a unique candidate uuid assigned by system
	ID string

	// nodeID represents the id assigned by registry
	nodeID string

	// leader stores true if current node is leader
	leader bool

	// Leader election config
	config Config

	// registry being used for leader election
	registry registry.IRegistry

	errCh chan error
}

// Run starts the leader election loop. Run will not return
// before leader election loop is stopped by ctx or it has
// stopped holding the leader lease
func (c *Candidate) Run(ctx context.Context) error {
	var err error

	logger.Ctx(ctx).Info("registering node with registry")
	c.nodeID, err = c.registry.Register(c.config.Name, c.config.LeaseDuration)
	if err != nil {
		logger.Ctx(ctx).Errorw("node registering failed with error", "error", err.Error())
		return err
	}

	ctx = context.WithValue(ctx, "registrationId", c.nodeID)

	logger.Ctx(ctx).Infow("acquiring node key", "key", c.config.NodePath)
	acquired := c.registry.Acquire(c.nodeID, c.config.NodePath, time.Now().String())

	if !acquired {
		return fmt.Errorf("failed to acquire node key : %s", c.config.NodePath)
	}

	grp, gctx := errgroup.WithContext(ctx)

	// Renew session periodically
	grp.Go(func() error {
		logger.Ctx(ctx).Infow("staring renew process for node key", "key", c.config.NodePath)
		return c.registry.RenewPeriodic(c.nodeID, c.config.LeaseDuration, gctx.Done())
	})

	// watch the leader key
	var leaderWatch registry.IWatcher
	grp.Go(func() error {
		logger.Ctx(ctx).Infow("setting up watch on leader key", "key", c.config.LockPath)
		leaderWatch, err = c.registry.Watch(gctx, &registry.WatchConfig{
			WatchType: "key",
			WatchPath: c.config.LockPath,
			Handler:   c.handler,
		})

		if err != nil {
			logger.Ctx(ctx).Infow("error creating leader watch", "error", err.Error())
			return err
		}

		logger.Ctx(ctx).Infow("starting watch on leader key", "key", c.config.LockPath)
		return leaderWatch.StartWatch()
	})

	grp.Go(func() error {
		select {
		case <-gctx.Done():
			logger.Ctx(gctx).Info("leader election context returned done")
			err = gctx.Err()
		case err = <-c.errCh:
			logger.Ctx(gctx).Info("leader election error channel received signal")
		}

		if leaderWatch != nil {
			leaderWatch.StopWatch()
		}

		return err
	})

	err = grp.Wait()
	if err != nil {
		logger.Ctx(gctx).Infow("leader election run exiting with err", "error", err.Error())
		c.release(gctx)
	}
	return err
}

// handler implements the handler calls from registry for events on leader key changes
func (c *Candidate) handler(ctx context.Context, result []registry.Pair) {
	logger.Ctx(ctx).Info("leader election handler called")
	if len(result) > 0 && result[0].SessionId != "" {
		logger.Ctx(ctx).Info("leader election key already locked")
		return
	}

	logger.Ctx(ctx).Info("leader election attempting to acquire key")
	acquired := c.registry.Acquire(c.nodeID, c.config.LockPath, time.Now().String())
	if acquired {
		logger.Ctx(ctx).Info("leader election acquire success")
		c.leader = true
		err := c.config.Callbacks.OnStartedLeading(ctx)

		if err != nil {
			logger.Ctx(ctx).Errorw("error from leader callback", "error", err.Error())
			c.errCh <- err
		}
	}
}

// IsLeader returns true if the last observed leader was this client else returns false.
func (c *Candidate) IsLeader() bool {
	return c.leader
}

// release attempts to release the leader lease if we have acquired it.
func (c *Candidate) release(ctx context.Context) bool {
	if c.nodeID == "" {
		return true
	}

	// deregister node, which releases lock as well
	if err := c.registry.Deregister(c.nodeID); err != nil {
		logger.Ctx(ctx).Error("Failed to deregister node: %v", err)
		return false
	}

	if c.IsLeader() {
		// handle OnStoppedLeading callback
		c.config.Callbacks.OnStoppedLeading()
	}

	// reset nodeId as current node id is deregistered
	c.nodeID = ""
	c.leader = false

	logger.Ctx(ctx).Info("successfully released lease")
	return true
}
