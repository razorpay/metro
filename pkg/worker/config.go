package worker

import (
	"time"
)

const (
	DefaultName        = "default"
	DefaultConcurrency = 1
	DefaultWaitTime    = 30 * time.Second
	DefaultRetryDelay  = 30 * time.Second
)

// config defined worker demon configuration
type Config struct {
	// Name identifier for the registered job
	// which has to be performed by the worker
	Name string

	// MaxConcurrency defined number of goroutine performing the operation
	MaxConcurrency int

	// WaitTime determines how much time to wait before poling for Message
	// if in case no Message is received from the queue
	// unit seconds
	WaitTime time.Duration

	// RetryDelay default delay for the job in case of any failure
	RetryDelay time.Duration
}

// SetDefaults sets the default value the config attributes
// if no value is provided for them
func (c *Config) SetDefaults() {
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = DefaultConcurrency
	}

	if c.WaitTime == 0 {
		c.WaitTime = DefaultWaitTime
	}

	if c.Name == "" {
		c.Name = DefaultName
	}

	if c.RetryDelay == 0 {
		c.RetryDelay = DefaultRetryDelay
	}
}
