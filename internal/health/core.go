package health

import (
	"context"
	"sync"

	"github.com/razorpay/metro/pkg/logger"
)

// Checker interface for health
type Checker interface {
	name() string
	checkHealth(ctx context.Context) (bool, error)
}

// Core holds business logic and/or orchestrator of other things in the package.
type Core struct {
	isMarkedUnhealthy bool
	mutex             *sync.Mutex
	checkers          []Checker
}

// ICore is an interface over health core
type ICore interface {
	IsHealthy(ctx context.Context) bool
	MarkUnhealthy()
}

// NewCore creates a new health core.
func NewCore(checkers ...Checker) (ICore, error) {
	return &Core{isMarkedUnhealthy: false, mutex: &sync.Mutex{}, checkers: checkers}, nil
}

// IsHealthy checks if the app has been marked unhealthy. If so it'll return false. Otherwise, it'll check the application
// health and return a boolean value based on the health check result.
func (c *Core) IsHealthy(ctx context.Context) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Server has been marked as unhealthy. No point in doing a health check.
	if c.isMarkedUnhealthy {
		return false
	}

	if c.checkers == nil {
		return true
	}

	for _, checker := range c.checkers {
		if checker == nil {
			continue
		}
		isHealthy, err := checker.checkHealth(ctx)
		if !isHealthy || err != nil {
			logger.Ctx(context.TODO()).Errorw("health check failed", "service_to_check", checker.name(), "error", err)
			return false
		}
	}

	return true
}

// MarkUnhealthy marks the core as unhealthy. Any further health checks after marking unhealthy will fail.
func (c *Core) MarkUnhealthy() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.isMarkedUnhealthy = true
}
