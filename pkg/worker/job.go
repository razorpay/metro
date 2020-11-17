package worker

import (
	"context"
	"time"
)

const (
	DefaultTimeout = 30 * time.Second
)

type IJob interface {
	// GetQueue should provide the queue name
	GetQueue() string

	// BeforeHandle will be called right before handle
	// this is responsible for accommodating any prerequisites if required
	// and it gives the context back
	BeforeHandle(context.Context) context.Context

	// handle handler function definition
	Handle(context.Context) error

	// GetName name to register this job with
	GetName() string

	// GetAttempt given count of how many times the Message is attempted for processing
	GetAttempt() int

	// GetMaxRetries gives maximum retry count
	// in case of failure in job processing
	GetMaxRetries() int

	// GetTimeout give the job timeout duration
	// within which the job should complete
	// else the job will be abandoned and retried again
	GetTimeout() time.Duration

	// IncrementAttempt will increment the attempt count
	IncrementAttempt()

	// OnSuccess will be called when the job processing is completed successfully
	OnSuccess(context.Context)

	// OnError will be called when the job processing is resulted in error
	OnError(context.Context, error)
}

// Job to be processed by a Manager
type Job struct {
	// queue the job should be placed into
	QueueName string

	// Name that will be registered with the worker
	Name string

	// MaxRetries defined max number of retries that can be performed
	// in case if job processing failure
	MaxRetries int

	// Attempt keeps track of number of times the job attempted for processing
	Attempt int

	// Timeout run time defined per job.
	// minimum timeout value is 30 sec
	Timeout time.Duration
}

// GetQueue should provide the queue name
func (j *Job) GetQueue() string {
	return j.QueueName
}

// GetName name to register this job with
func (j *Job) GetName() string {
	return j.Name
}

// GetMaxRetries gives maximum retry count
// in case of failure in job processing
func (j *Job) GetMaxRetries() int {
	return j.MaxRetries
}

// GetAttempt given count of how many times the Message is attempted for processing
func (j *Job) GetAttempt() int {
	return j.Attempt
}

// Handle handler function definition
func (j *Job) Handle(context.Context) error {
	panic("handle method is not implemented")
}

// IncrementAttempt will increment the attempt count
func (j *Job) IncrementAttempt() {
	j.Attempt++
}

// GetTimeout give the job timeout duration
// within which the job should complete
// else the job will be abandoned and retried again
func (j *Job) GetTimeout() time.Duration {
	if j.Timeout == 0 {
		return DefaultTimeout
	}

	return j.Timeout
}

// OnSuccess will be called when the job processing is completed successfully
func (j *Job) OnSuccess(ctx context.Context) {
	// on success behavior to be implemented by the respective job
}

// OnError will be called when the job processing is resulted in error
func (j *Job) OnError(ctx context.Context, err error) {
	// on error behavior to be implemented by the respective job
}

// BeforeHandle will be called right before handle
// this is responsible for accommodating any prerequisites if required
// and it gives the context back
func (j *Job) BeforeHandle(ctx context.Context) context.Context {
	// before handle behavior to be implemented by the respective job
	return ctx
}
