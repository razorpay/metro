package tasks

import "context"

// IManager defines worker interface
type IManager interface {
	// Start the worker with the given context
	Start(ctx context.Context) error

	// Stop the worker
	Stop(ctx context.Context)
}

type Option func(e IManager)
