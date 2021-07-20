package leaderelection

import (
	"context"
	"fmt"
)

// Config for leaderelector
type Config struct {
	// LockPath is key in registry that will be used for locking
	LockPath string

	// Callbacks are callbacks that are triggered during certain lifecycle
	// events of the LeaderElector
	Callbacks LeaderCallbacks
}

// Errors raised by package
var (
	// ErrInvalidOnStartedLeadingCallback is thrown if OnStartedLeading callback is not defined
	ErrInvalidOnStartedLeadingCallback = fmt.Errorf("OnStartedLeading callback must not be nil")

	// ErrInvalidOnStoppedLeadingCallback is thrown if OnStoppedLeading callback is not defined
	ErrInvalidOnStoppedLeadingCallback = fmt.Errorf("OnStoppedLeading callback must not be nil")

	// ErrInvalidLockPath is thrown if lock path for lease is not defined
	ErrInvalidLockPath = fmt.Errorf("lock path must not be nil")
)

// Validate validates the config
func (c *Config) Validate() error {
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
