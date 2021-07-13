package leaderelection

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// New creates a instance of LeaderElector from a LeaderElection Config
func New(id string, sessionID string, c Config, registry registry.IRegistry) (*Candidate, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	if id == "" {
		return nil, errors.New("candidate id is nil")
	}

	if sessionID == "" {
		return nil, errors.New("sessionID is nil")
	}

	le := Candidate{
		id:        id,
		leaderID:  "",
		sessionID: sessionID,
		config:    c,
		registry:  registry,
		errCh:     make(chan error),
	}

	return &le, nil
}

// Candidate is a leader election candidate.
type Candidate struct {
	// id of the current pod
	id string

	// sessionID current session
	sessionID string

	// leader stores the id of the current leader
	leaderID string

	// Leader election config
	config Config

	// registry being used for leader election
	registry registry.IRegistry

	// errCh for caching errors in handlers
	errCh chan error
}

// Run starts the leader election loop. Run will not return
// before leader election loop is stopped by ctx or it has
// stopped holding the leader lease
func (c *Candidate) Run(ctx context.Context) error {
	logger.Ctx(ctx).Infow("setting up watch on leader key", "key", c.config.LockPath)

	// watch the leader key
	leaderWatch, err := c.registry.Watch(ctx, &registry.WatchConfig{
		WatchType: "key",
		WatchPath: c.config.LockPath,
		Handler:   c.handler,
	})

	if err != nil {
		logger.Ctx(ctx).Infow("error creating leader watch", "error", err.Error())
		return err
	}

	grp, gctx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		logger.Ctx(ctx).Infow("starting watch on leader key", "key", c.config.LockPath)
		return leaderWatch.StartWatch()
	})

	grp.Go(func() error {
		select {
		case <-gctx.Done():
			logger.Ctx(gctx).Info("leader election context returned done")
			err = gctx.Err()
		case err = <-c.errCh:
			logger.Ctx(gctx).Info("error in leader election handler")
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
		c.leaderID = string(result[0].Value)
		return
	}

	logger.Ctx(ctx).Info("leader election attempting to acquire key")
	acquired, aerr := c.registry.Acquire(ctx, c.sessionID, c.config.LockPath, []byte(c.id))
	if aerr != nil {
		logger.Ctx(ctx).Errorw("failed to acquire node key", "key", c.id, "error", aerr.Error())
		c.errCh <- aerr
	}

	if acquired {
		logger.Ctx(ctx).Info("leader election acquire success")
		c.leaderID = c.id
		go func() {
			err := c.config.Callbacks.OnStartedLeading(ctx)

			if err != nil {
				c.errCh <- err
			}
		}()

	}
}

// IsLeader returns true if the last observed leader was this client else returns false.
func (c *Candidate) IsLeader() bool {
	return c.id == c.leaderID
}

// release attempts to release the leader lease if we have acquired it.
func (c *Candidate) release(ctx context.Context) bool {
	// release the lease on leader path
	if ok := c.registry.Release(ctx, c.sessionID, c.config.LockPath, c.id); !ok {
		logger.Ctx(ctx).Warnw("Failed to release the leader lock", "candidate.id", c.id)
		return false
	}

	if c.IsLeader() {
		// handle OnStoppedLeading callback
		c.config.Callbacks.OnStoppedLeading(ctx)
	}

	logger.Ctx(ctx).Info("successfully released lease")
	return true
}
