package leaderelection

import (
	"context"
	"encoding/json"
	"time"

	"github.com/razorpay/metro/internal/node"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	"golang.org/x/sync/errgroup"
)

// New creates a instance of LeaderElector from a LeaderElection Config
func New(model *node.Model, c Config, registry registry.IRegistry) (*Candidate, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	le := Candidate{
		node:      model,
		config:    c,
		registry:  registry,
		sessionID: "",
		leader:    false,
		errCh:     make(chan error),
	}

	return &le, nil
}

// Candidate is a leader election candidate.
type Candidate struct {
	// node with unique uuid assigned by system
	node *node.Model

	// sessionID registered with registered for current session
	sessionID string

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
	c.sessionID, err = c.registry.Register(c.config.Name, c.config.LeaseDuration)
	if err != nil {
		logger.Ctx(ctx).Errorw("node registering failed with error", "error", err.Error())
		return err
	}

	logger.Ctx(ctx).Infow("acquiring node key", "key", c.node.Key())

	value, merr := json.Marshal(c.node)
	if merr != nil {
		logger.Ctx(ctx).Errorw("error converting node model to json", "error", err.Error())
	}
	acquired, aerr := c.registry.Acquire(c.sessionID, c.node.Key(), value)

	if aerr != nil || !acquired {
		logger.Ctx(ctx).Errorw("failed to acquire node key", "key", c.node.Key(), "error", aerr.Error())
		return aerr
	}

	grp, gctx := errgroup.WithContext(ctx)

	// Renew session periodically
	grp.Go(func() error {
		logger.Ctx(ctx).Infow("staring renew process for node key", "key", c.node.Key())
		return c.registry.RenewPeriodic(c.sessionID, c.config.LeaseDuration, gctx.Done())
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
	if len(result) > 0 && result[0].SessionID != "" {
		logger.Ctx(ctx).Info("leader election key already locked")
		return
	}

	logger.Ctx(ctx).Info("leader election attempting to acquire key")
	acquired, aerr := c.registry.Acquire(c.sessionID, c.config.LockPath, []byte(time.Now().String()))
	if aerr != nil {
		logger.Ctx(ctx).Errorw("failed to acquire node key", "key", c.node.Key(), "error", aerr.Error())
		c.errCh <- aerr
	}

	if acquired {
		logger.Ctx(ctx).Info("leader election acquire success")
		c.leader = true
		err := c.config.Callbacks.OnStartedLeading(ctx)

		if err != nil {
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
	if c.sessionID == "" {
		return true
	}

	// deregister node, which releases lock as well
	if err := c.registry.Deregister(c.sessionID); err != nil {
		logger.Ctx(ctx).Error("Failed to deregister node: %v", err)
		return false
	}

	if c.IsLeader() {
		// handle OnStoppedLeading callback
		c.config.Callbacks.OnStoppedLeading()
	}

	// reset sessionID as current session is expired
	c.sessionID = ""
	c.leader = false

	logger.Ctx(ctx).Info("successfully released lease")
	return true
}
