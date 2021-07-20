package service

import "context"

// IService interface is implemented by various services
type IService interface {
	// Start the service, it should be implemented as a blocking call
	// method should return gracefully when ctx is Done
	Start(ctx context.Context) error
}
