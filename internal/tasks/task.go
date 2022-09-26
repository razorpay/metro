package tasks

import "context"

// ITask defines worker interface
type ITask interface {
	// Run the worker with the given context
	Run(ctx context.Context) error
}

// A Option is an option for tasks
type Option func(task ITask)
