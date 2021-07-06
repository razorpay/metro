package service

import "context"

// IService interface is implemented by various services
type IService interface {
	// Start the service, it should be implemented as a blocking call
	// method should return when ctx is Done
	Start(ctx context.Context) error

	// Stop the service, it should clean up all the go routines/tasks in start
	Stop(ctx context.Context)
}
