package leaderelection

import (
	"fmt"

	"github.com/razorpay/metro/pkg/registry"
)

const (
	JitterFactor = 1.2
)

// NewLeaderElector creates a LeaderElector from a LeaderElection Config
func NewLeaderElector(c *Config, registry registry.Registry) (*LeaderElector, error) {
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
	config   *Config
	registry registry.Registry
}
