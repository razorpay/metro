package leaderelection

import (
	"context"
	"fmt"

	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/pkg/registry"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog"
)

const (
	JitterFactor = 1.2
)

// NewLeaderElector creates a LeaderElector from a LeaderElection Config
func NewLeaderElector(c Config, registry registry.Registry) (*LeaderElector, error) {
	if c.LeaseDuration <= c.RenewDeadline {
		return nil, fmt.Errorf("leaseDuration must be greater than renewDeadline")
	}
	if c.LeaseDuration < 1 {
		return nil, fmt.Errorf("leaseDuration must be greater than zero")
	}
	if c.RenewDeadline < 1 {
		return nil, fmt.Errorf("renewDeadline must be greater than zero")
	}

	if c.Callbacks.OnStartedLeading == nil {
		return nil, fmt.Errorf("OnStartedLeading callback must not be nil")
	}
	if c.Callbacks.OnStoppedLeading == nil {
		return nil, fmt.Errorf("OnStoppedLeading callback must not be nil")
	}

	if c.Path == "" {
		return nil, fmt.Errorf("path must not be nil")
	}
	le := LeaderElector{
		config:   c,
		registry: registry,
	}

	return &le, nil
}

// LeaderElector is a leader election client.
type LeaderElector struct {
	config   Config
	registry registry.Registry
	NodeId   string
	LeaderId string
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

// RunOrDie starts a client with the provided config or panics if the config
// fails to validate. RunOrDie blocks until leader election loop is
// stopped by ctx or it has stopped holding the leader lease
func RunOrDie(ctx context.Context, lec Config, registry registry.Registry) {
	le, err := NewLeaderElector(lec, registry)
	if err != nil {
		panic(err)
	}
	le.Run(ctx)
}

// GetLeader returns the identity of the last observed leader or returns the empty string if
// no leader has yet been observed.
func (le *LeaderElector) GetLeader() string {
	return le.NodeId
}

// IsLeader returns true if the last observed leader was this client else returns false.
func (le *LeaderElector) IsLeader() bool {
	return le.NodeId == le.LeaderId
}

// acquire loops calling tryAcquireOrRenew and returns true immediately when tryAcquireOrRenew succeeds.
// Returns false if ctx signals done.
func (le *LeaderElector) acquire(ctx context.Context) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	succeeded := false

	logger.Log.Info("attempting to acquire leader lease")
	wait.JitterUntil(func() {
		succeeded = le.tryAcquireOrRenew(ctx)
		le.maybeReportTransition()
		if !succeeded {
			logger.Log.Info("failed to acquire lease")
			return
		}

		logger.Log.Info("successfully acquired lease")
		cancel()
	}, le.config.RetryPeriod, JitterFactor, true, ctx.Done())
	return succeeded
}

// renew loops calling tryAcquireOrRenew and returns immediately when tryAcquireOrRenew fails or ctx signals done.
func (le *LeaderElector) renew(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wait.Until(func() {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, le.config.RenewDeadline)
		defer timeoutCancel()
		err := wait.PollImmediateUntil(le.config.RetryPeriod, func() (bool, error) {
			return le.tryAcquireOrRenew(timeoutCtx), nil
		}, timeoutCtx.Done())

		le.maybeReportTransition()

		if err == nil {
			logger.Log.Info("successfully renewed lease")
			return
		}
		logger.Log.Info("failed to renew lease : %v", err)
		cancel()
	}, le.config.RetryPeriod, ctx.Done())

	// if we hold the lease, give it up
	le.release()
}

// release attempts to release the leader lease if we have acquired it.
func (le *LeaderElector) release() bool {
	if !le.IsLeader() {
		return true
	}

	// release lock on resource
	if err := le.registry.Release("", "", ""); err != nil {
		logger.Log.Error("Failed to release lock: %v", err)
		return false
	}

	return true
}

// tryAcquireOrRenew tries to acquire a leader lease if it is not already acquired,
// else it tries to renew the lease if it has already been acquired. Returns true
// on success else returns false.
func (le *LeaderElector) tryAcquireOrRenew(ctx context.Context) bool {
	// 1. obtain or create the ElectionRecord
	// fetch call from registery
	var err error
	if err != nil {
		if !errors.IsNotFound(err) {
			logger.Log.Errorf("error retrieving resource lock: %v", err)
			return false
		}

		// create resouse
		if err = le.registry.Acquire("", "", ""); err != nil {
			logger.Log.Errorf("error acquiring leader election lock: %v", err)
			return false
		}
		le.LeaderId = le.NodeId
		return true
	}

	// 2. Record obtained, Renew lock
	if err = le.registry.Renew(""); err != nil {
		klog.Errorf("Failed to update lock: %v", err)
		return false
	}

	le.LeaderId = le.NodeId
	return true
}

func (le *LeaderElector) maybeReportTransition() {
	if le.NodeId == le.LeaderId {
		return
	}

	if le.config.Callbacks.OnNewLeader != nil {
		go le.config.Callbacks.OnNewLeader(le.LeaderId)
	}
}
