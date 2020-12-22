package leaderelection

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"

	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/pkg/registry"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// JitterFactor used in wait utils
	JitterFactor = 1.2
)

// NewLeaderElector creates a LeaderElector from a LeaderElection Config
func NewLeaderElector(c Config, registry registry.Registry) (*LeaderElector, error) {
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
	registry registry.Registry
}

// Run starts the leader election loop. Run will not return
// before leader election loop is stopped by ctx or it has
// stopped holding the leader lease
func (le *LeaderElector) Run(ctx context.Context) {
	defer runtime.HandleCrash()
	defer func() {
		le.config.Callbacks.OnStoppedLeading()
	}()

	if !le.acquire(ctx) {
		return // ctx signalled done
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go le.config.Callbacks.OnStartedLeading(ctx)
	le.renew(ctx)
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

// acquire loops calling tryAcquire and returns true immediately when tryAcquire succeeds.
// Returns false if ctx signals done.
func (le *LeaderElector) acquire(ctx context.Context) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	succeeded := false

	logger.Log.Info("attempting to acquire leader lease")
	wait.JitterUntil(func() {
		succeeded = le.tryAcquireOrRenew(ctx)
		//le.maybeReportTransition()
		if !succeeded {
			logger.Log.Info("failed to acquire lease")
			return
		}

		logger.Log.Info("successfully acquired lease")
		cancel()
	}, le.config.RetryPeriod, JitterFactor, true, ctx.Done())
	return succeeded
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
			logger.Log.Info("node is unregisterd")
			le.nodeID = ""
		} else {
			err = le.registry.Renew(le.nodeID)
			if err != nil {
				logger.Log.Infof("error while renewing registration: %v", err.Error())
				return false
			}
		}
	}

	// if not registered, register it
	if le.nodeID == "" {
		le.nodeID, err = le.registry.Register(le.config.Name, le.config.LeaseDuration)
		if err != nil {
			logger.Log.Errorf("failed to register node with registry: %v", err)
			return false
		}
		logger.Log.Infof("succesfully registered node with registry: %s", le.nodeID)
	}

	// try acquiring lock
	acquired := le.registry.Acquire(le.nodeID, le.config.Path, time.Now().String())

	if acquired {
		// acquired success
		le.leaderID = le.nodeID
	}
	return acquired
}

// renew loops calling tryRenew and returns immediately when tryRenew fails or ctx signals done
func (le *LeaderElector) renew(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wait.Until(func() {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, le.config.RenewDeadline)
		defer timeoutCancel()
		err := wait.PollImmediateUntil(le.config.RetryPeriod, func() (bool, error) {
			return le.tryAcquireOrRenew(timeoutCtx), nil
		}, timeoutCtx.Done())

		//le.maybeReportTransition()

		if err == nil {
			logger.Log.Info("successfully renewed lease")
			return
		}
		logger.Log.Info("failed to renew lease : %v", err)
		cancel()
	}, le.config.RenewDeadline, ctx.Done())

	// if we hold the lease, give it up
	le.release()
}

// release attempts to release the leader lease if we have acquired it.
func (le *LeaderElector) release() bool {
	if !le.IsLeader() {
		return true
	}

	// deregister node, which releases lock as well
	if err := le.registry.Deregister(le.nodeID); err != nil {
		logger.Log.Error("Failed to deregister node: %v", err)
		return false
	}

	return true
}

//func (le *LeaderElector) maybeReportTransition() {
//	if le.nodeID == le.leaderID {
//		return
//	}
//
//	if le.config.Callbacks.OnNewLeader != nil {
//		go le.config.Callbacks.OnNewLeader(le.leaderID)
//	}
//}

// RunOrDie starts a client with the provided config or panics if the config
// fails to validate. RunOrDie blocks until leader election loop is
// stopped by ctx or it has stopped holding the leader lease
func RunOrDie(ctx context.Context, lec Config, registry registry.Registry) {
	le, err := NewLeaderElector(lec, registry)
	if err != nil {
		// TODO: return error than panic
		panic(err)
	}
	le.Run(ctx)
}
