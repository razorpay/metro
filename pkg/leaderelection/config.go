package leaderelection

import (
	"context"
	"fmt"
	"time"
)

// Config for leaderelector
type Config struct {
	// LockPath is key in registry that will be used for locking
	LockPath string

	// LeaseDuration is the duration that follower candidates will
	// wait to force acquire leadership. This is measured against time of
	// last observed ack.
	//
	// Core clients default this value to 15 seconds.
	LeaseDuration time.Duration

	// Callbacks are callbacks that are triggered during certain lifecycle
	// events of the LeaderElector
	Callbacks LeaderCallbacks

	// Name of the leaderelector service
	Name string
}

// Errors raised by package
var (
	// ErrInvalidLeaseDuration is thrown if lease duration is less than 0
	ErrInvalidLeaseDuration = fmt.Errorf("leaseDuration must be greater than zero")

	// ErrInvalidOnStartedLeadingCallback is thrown if OnStartedLeading callback is not defined
	ErrInvalidOnStartedLeadingCallback = fmt.Errorf("OnStartedLeading callback must not be nil")

	// ErrInvalidOnStoppedLeadingCallback is thrown if OnStoppedLeading callback is not defined
	ErrInvalidOnStoppedLeadingCallback = fmt.Errorf("OnStoppedLeading callback must not be nil")

	// ErrInvalidLockPath is thrown if lock path for lease is not defined
	ErrInvalidLockPath = fmt.Errorf("lock path must not be nil")
)

// Validate validates the config
func (c *Config) Validate() error {
	if c.LeaseDuration < 1 {
		return ErrInvalidLeaseDuration
	}
	if c.Callbacks.OnStartedLeading == nil {
		return ErrInvalidOnStartedLeadingCallback
	}
	if c.Callbacks.OnStoppedLeading == nil {
		return ErrInvalidOnStoppedLeadingCallback
	}
	if c.LockPath == "" {
		return ErrInvalidLockPath
	}

	return nil
}

// LeaderCallbacks are callbacks that are triggered during certain
// lifecycle events of the LeaderElector. These are invoked asynchronously.
type LeaderCallbacks struct {
	// OnStartedLeading is called when a LeaderElector client starts leading
	OnStartedLeading func(context.Context) error
	// OnStoppedLeading is called when a LeaderElector client stops leading
	OnStoppedLeading func(ctx context.Context)
}
