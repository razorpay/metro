package tasks

import "context"

// IManager defines worker interface
type IManager interface {
	// Start the worker with the given context
	Start(ctx context.Context) error

	// Stop the worker
	Stop(ctx context.Context)
}

// A Option is an option for manager tasks
type Option func(e IManager)
