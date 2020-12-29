package leaderelection

import (
	"context"
	"fmt"
	"time"
)

// Config for leaderelector
type Config struct {
	// Path to the location in registry that will be used for locking
	Path string

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

// Validate validates the config
func (c *Config) Validate() error {
	if c.LeaseDuration <= c.RenewDeadline {
		return fmt.Errorf("leaseDuration must be greater than renewDeadline")
	}

	if c.LeaseDuration < 1 {
		return fmt.Errorf("leaseDuration must be greater than zero")
	}

	if c.RenewDeadline < 1 {
		return fmt.Errorf("renewDeadline must be greater than zero")
	}

	if c.Callbacks.OnStartedLeading == nil {
		return fmt.Errorf("OnStartedLeading callback must not be nil")
	}
	if c.Callbacks.OnStoppedLeading == nil {
		return fmt.Errorf("OnStoppedLeading callback must not be nil")
	}

	if c.RetryPeriod < 1 {
		return fmt.Errorf("retryPeriod must be greater than zero")
	}

	if c.Path == "" {
		return fmt.Errorf("path must not be nil")
	}

	return nil
}

// LeaderCallbacks are callbacks that are triggered during certain
// lifecycle events of the LeaderElector. These are invoked asynchronously.
type LeaderCallbacks struct {
	// OnStartedLeading is called when a LeaderElector client starts leading
	OnStartedLeading func(ctx context.Context)
	// OnStoppedLeading is called when a LeaderElector client stops leading
	OnStoppedLeading func()
}
