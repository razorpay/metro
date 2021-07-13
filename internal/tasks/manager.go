package tasks

import "context"

// IManager defines worker interface
type IManager interface {
	// Run the worker with the given context
	Run(ctx context.Context) error
}

// A Option is an option for manager tasks
type Option func(e IManager)
