package leaderelection

import (
	"context"
	"time"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// JitterFactor used in wait utils
	JitterFactor = 1.2
)

// New creates a instance of LeaderElector from a LeaderElection Config
func New(c Config, registry registry.IRegistry) (*Candidate, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	le := Candidate{
		config:   c,
		registry: registry,
		nodeID:   "",
		leader:   false,
	}

	return &le, nil
}

// Candidate is a leader election candidate.
type Candidate struct {
	// nodeID represents the id assigned by registry
	nodeID string

	// leader stores true if current node is leader
	leader bool

	// Leader election config
	config Config

	// registry being used for leader election
	registry registry.IRegistry
}

// Run starts the leader election loop. Run will not return
// before leader election loop is stopped by ctx or it has
// stopped holding the leader lease
func (c *Candidate) Run(ctx context.Context) error {
	leadCtx, leadCancel := context.WithCancel(ctx)
	defer leadCancel()

	logger.Ctx(ctx).Info("attempting to acquire leader lease")
	// outer wait loop runs when a instance has successfully acquired lease
	wait.Until(func() {
		retryCtx, retryCancel := context.WithTimeout(ctx, c.config.RenewDeadline)
		defer retryCancel()

		// inner wait retry loop for faster retries if failed to acquire lease
		acquired := false
		wait.JitterUntil(func() {
			acquired = c.tryAcquireOrRenew(retryCtx)

			if !acquired {
				logger.Ctx(retryCtx).Infow("failed to acquire lease, retrying...", "nodeID", c.nodeID)
				return
			}

			// if succeeded, we can break the inner loop by cancelling context and renew in outer loop
			logger.Ctx(retryCtx).Infow("successfully acquired lease", "nodeID", c.nodeID)
			retryCancel()
		}, c.config.RetryPeriod, JitterFactor, true, retryCtx.Done())

		// OnStartedLeading if new leader
		if acquired && !c.leader {
			c.leader = true
			go c.config.Callbacks.OnStartedLeading(leadCtx)
		}
	}, c.config.RenewDeadline, ctx.Done())

	// context returned done, release the lease
	logger.Ctx(ctx).Info("context done, releasing lease")
	c.release(ctx)
	return nil
}

// IsLeader returns true if the last observed leader was this client else returns false.
func (c *Candidate) IsLeader() bool {
	return c.leader
}

// tryAcquire tries to acquire a leader lease if it is not already acquired
// Returns true on success else returns false.
func (c *Candidate) tryAcquireOrRenew(ctx context.Context) bool {
	var err error

	// validate if node is already registered
	if c.nodeID != "" {
		// if node is not registred anymore, we will need register it again
		// it can happen that ttl associated with registration had expired
		if ok := c.registry.IsRegistered(c.nodeID); !ok {
			logger.Ctx(ctx).Infof("node %s is unregisterd", c.nodeID)
			c.nodeID = ""
		} else {
			err = c.registry.Renew(c.nodeID)
			if err != nil {
				logger.Ctx(ctx).Infof("error while renewing registration: %v", err.Error())
				return false
			}
		}
	}

	// if not registered, register it
	if c.nodeID == "" {
		c.nodeID, err = c.registry.Register(c.config.Name, c.config.LeaseDuration)
		if err != nil {
			logger.Ctx(ctx).Errorf("failed to register node with registry: %v", err)
			return false
		}
		logger.Ctx(ctx).Infow("succesfully registered node with registry", "nodeID", c.nodeID)
	}

	// try acquiring lock
	acquired := c.registry.Acquire(c.nodeID, c.config.Path, time.Now().String())

	return acquired
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

	logger.Ctx(ctx).Info("successfully released lease")
	return true
}
