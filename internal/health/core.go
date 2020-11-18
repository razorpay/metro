package health

import (
	"context"
	"sync"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/pkg/spine/db"
)

type Checker interface {
	CheckHealth() (bool, error)
}

// Core holds business logic and/or orchestrator of other things in the package.
type Core struct {
	isMarkedUnhealthy bool
	mutex             *sync.Mutex
	checkers          []Checker
}

// NewCore creates a new health core.
func NewCore(checkers ...Checker) (*Core, error) {
	return &Core{isMarkedUnhealthy: false, mutex: &sync.Mutex{}, checkers: checkers}, nil
}

// RunHealthCheck runs various server checks and returns true if all individual components are working fine.
// Todo: Fix server check response per https://tools.ietf.org/id/draft-inadarei-api-health-check-01.html :)
// IsHealthy checks if the app has been marked unhealthy. If so it'll return false. Otherwise, it'll check the application
// health and return a boolean value based on the health check result.
func (c *Core) IsHealthy() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Server has been marked as unhealthy. No point in doing a health check.
	if c.isMarkedUnhealthy {
		return false
	}

	for _, checker := range c.checkers {
		isHealthy, err := checker.CheckHealth()
		if !isHealthy {
			boot.Logger(context.TODO()).WithError(err).Error("health check failed")
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

type DBHealthChecker struct {
	db *db.DB
}

func (d *DBHealthChecker) CheckHealth() (bool, error) {
	err := d.db.Alive()
	if err != nil {
		return false, err
	}

	return true, err
}

func NewDBHealthChecker(db *db.DB) Checker {
	return &DBHealthChecker{db: db}
}
