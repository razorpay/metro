package leaderelection

import (
	"context"
	"fmt"
	"time"
)

// Config for leaderelector
type Config struct {
	// NodePath is key in registry that will be used for node registration
	NodePath string

	// LockPath is key in registry that will be used for locking
	LockPath string

	// LeaseDuration is the duration that follower candidates will
	// wait to force acquire leadership. This is measured against time of
	// last observed ack.
	//
	// Core clients default this value to 15 seconds.
	LeaseDuration time.Duration

	// RenewDeadline is the duration that the acting master will retry
	// refreshing leadership before giving up.
	RenewDeadline time.Duration

	// RetryPeriod is the duration the LeaderElector clients should wait
	// between tries of actions.
	//
	// Core clients default this value to 2 seconds.
	RetryPeriod time.Duration

	// Callbacks are callbacks that are triggered during certain lifecycle
	// events of the LeaderElector
	Callbacks LeaderCallbacks

	// Name of the leaderelector service
	Name string
}

// Errors raised by package
var (
	// ErrLeaseDurationLessThanRenewDeadline is thrown if lease duration is lessthan renew deadline
	ErrLeaseDurationLessThanRenewDeadline = fmt.Errorf("leaseDuration must be greater than renewDeadline")

	// ErrInvalidLeaseDuration is thrown if lease duration is less than 0
	ErrInvalidLeaseDuration = fmt.Errorf("leaseDuration must be greater than zero")

	// ErrInvalidRenewDeadline is thrown if renew deadline is less than 0
	ErrInvalidRenewDeadline = fmt.Errorf("renewDeadline must be greater than zero")

	// ErrInvalidOnStartedLeadingCallback is thrown if OnStartedLeading callback is not defined
	ErrInvalidOnStartedLeadingCallback = fmt.Errorf("OnStartedLeading callback must not be nil")

	// ErrInvalidOnStoppedLeadingCallback is thrown if OnStoppedLeading callback is not defined
	ErrInvalidOnStoppedLeadingCallback = fmt.Errorf("OnStoppedLeading callback must not be nil")

	// ErrInvalidRetryPeriod is thrown if retry period is less than 0
	ErrInvalidRetryPeriod = fmt.Errorf("retryPeriod must be greater than zero")

	// ErrInvalidPath is thrown if path for lease is not defined
	ErrInvalidPath = fmt.Errorf("path must not be nil")
)

// Validate validates the config
func (c *Config) Validate() error {
	if c.LeaseDuration <= c.RenewDeadline {
		return ErrLeaseDurationLessThanRenewDeadline
	}

	if c.LeaseDuration < 1 {
		return ErrInvalidLeaseDuration
	}

	if c.RenewDeadline < 1 {
		return ErrInvalidRenewDeadline
	}

	if c.Callbacks.OnStartedLeading == nil {
		return ErrInvalidOnStartedLeadingCallback
	}
	if c.Callbacks.OnStoppedLeading == nil {
		return ErrInvalidOnStoppedLeadingCallback
	}

	if c.RetryPeriod < 1 {
		return ErrInvalidRetryPeriod
	}

	if c.LockPath == "" {
		return ErrInvalidPath
	}

	return nil
}

// LeaderCallbacks are callbacks that are triggered during certain
// lifecycle events of the LeaderElector. These are invoked asynchronously.
type LeaderCallbacks struct {
	// OnStartedLeading is called when a LeaderElector client starts leading
	OnStartedLeading func(context.Context) error
	// OnStoppedLeading is called when a LeaderElector client stops leading
	OnStoppedLeading func()
}
