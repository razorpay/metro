package tasks

import "context"

// ITask defines worker interface
type ITask interface {
	// Run the worker with the given context
	Run(ctx context.Context) error
}

// IPubTask defines Publisher worker interface
type IPubTask interface {
	// Run the worker with the given context
	Run(ctx context.Context) error
	// CheckIfTopicExists in the Topic Cache Data
	CheckIfTopicExists(ctx context.Context, topic string) bool
}

// A Option is an option for tasks
type Option func(task ITask)
