package leaderelection

import (
	"context"
	"time"
)

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
}

// LeaderCallbacks are callbacks that are triggered during certain
// lifecycle events of the LeaderElector. These are invoked asynchronously.
type LeaderCallbacks struct {
	// OnStartedLeading is called when a LeaderElector client starts leading
	OnStartedLeading func(context.Context)
	// OnStoppedLeading is called when a LeaderElector client stops leading
	OnStoppedLeading func()
	// OnNewLeader is called when the client observes a leader that is
	// not the previously observed leader. This includes the first observed
	// leader when the client starts.
	OnNewLeader func(identity string)
}
