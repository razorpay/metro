package leaderelection

import (
	"context"
	"sync"
	"time"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	"k8s.io/apimachinery/pkg/util/wait"
)

// NewLeaderElector creates a LeaderElector from a LeaderElection Config
func NewLeaderElector(c Config, registry registry.IRegistry) (*LeaderElector, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	le := LeaderElector{
		config:   c,
		registry: registry,
		nodeID:   "",
	}

	return &le, nil
}

// LeaderElector is a leader election client.
type LeaderElector struct {
	// nodeID represents the id assigned by registry
	nodeID string

	// leaderID stores id the current leader
	leaderID string

	// Leader election config
	config Config

	// registry being used for leader election
	registry registry.IRegistry
}

// Run starts the leader election loop. Run will not return
// before leader election loop is stopped by ctx or it has
// stopped holding the leader lease
func (le *LeaderElector) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	leadCtx, leadCancel := context.WithCancel(ctx)

	logger.Ctx(ctx).Info("attempting to acquire leader lease")
	// outer wait loop runs when a instance has successfully acquired lease
	wait.Until(func() {
		retryCtx, retryCancel := context.WithCancel(ctx)
		defer retryCancel()

		// inner wait retry loop for faster retries if failed to acquire lease
		acquired := false
		wait.Until(func() {
			acquired = le.tryAcquireOrRenew(retryCtx)

			if !acquired {
				logger.Ctx(retryCtx).Infow("failed to acquire lease, retrying...")
				return
			}

			// if succeeded, we can break the inner loop by cancelling context and renew in outer loop
			logger.Ctx(ctx).Info("successfully acquired lease")
			retryCancel()
		}, le.config.RetryPeriod, retryCtx.Done())

		// OnStartedLeading
		le.leaderID = le.nodeID
		go le.config.Callbacks.OnStartedLeading(leadCtx)

	}, le.config.RenewDeadline, ctx.Done())

	// context returned done, release the lease
	logger.Ctx(ctx).Info("context done, releasing lease")
	leadCancel()
	le.release(ctx)
}

// GetLeader returns the identity of the last observed leader or returns the empty string if
// no leader has yet been observed.
func (le *LeaderElector) GetLeader() string {
	return le.nodeID
}

// IsLeader returns true if the last observed leader was this client else returns false.
func (le *LeaderElector) IsLeader() bool {
	// TODO: fetch latest data from registry for current leader
	return (le.nodeID != "") && (le.nodeID == le.leaderID)
}

// tryAcquire tries to acquire a leader lease if it is not already acquired
// Returns true on success else returns false.
func (le *LeaderElector) tryAcquireOrRenew(ctx context.Context) bool {
	var err error

	// validate if node is already registered
	if le.nodeID != "" {
		// if node is not registred anymore, we will need register it again
		// it can happen that ttl associated with registration had expired
		if ok := le.registry.IsRegistered(le.nodeID); !ok {
			logger.Ctx(ctx).Infof("node %s is unregisterd", le.nodeID)
			le.nodeID = ""
		} else {
			err = le.registry.Renew(le.nodeID)
			if err != nil {
				logger.Ctx(ctx).Infof("error while renewing registration: %v", err.Error())
				return false
			}
		}
	}

	// if not registered, register it
	if le.nodeID == "" {
		le.nodeID, err = le.registry.Register(le.config.Name, le.config.LeaseDuration)
		if err != nil {
			logger.Ctx(ctx).Errorf("failed to register node with registry: %v", err)
			return false
		}
		logger.Ctx(ctx).Infof("succesfully registered node with registry: %s", le.nodeID)
	}

	// try acquiring lock
	acquired := le.registry.Acquire(le.nodeID, le.config.Path, time.Now().String())

	return acquired
}

// release attempts to release the leader lease if we have acquired it.
func (le *LeaderElector) release(ctx context.Context) bool {
	if !le.IsLeader() {
		return true
	}

	// deregister node, which releases lock as well
	if err := le.registry.Deregister(le.nodeID); err != nil {
		logger.Ctx(ctx).Error("Failed to deregister node: %v", err)
		return false
	}

	// reset nodeId as current node id is deregistered
	le.nodeID = ""

	// handle OnStoppedLeading callback
	le.config.Callbacks.OnStoppedLeading()

	logger.Ctx(ctx).Info("successfully released lease")
	return true
}
