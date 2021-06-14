package health

import (
	"context"
	"fmt"
	"sync"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
)

// Checker interface for health
type Checker interface {
	name() string
	checkHealth() (bool, error)
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

// IsHealthy checks if the app has been marked unhealthy. If so it'll return false. Otherwise, it'll check the application
// health and return a boolean value based on the health check result.
func (c *Core) IsHealthy() bool {
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
		isHealthy, err := checker.checkHealth()
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

type registryHealthChecker struct {
	service  string
	registry registry.IRegistry
}

func (r *registryHealthChecker) checkHealth() (bool, error) {
	return r.registry.IsAlive()
}

func (r *registryHealthChecker) name() string {
	return fmt.Sprintf("registry:%v", r.service)
}

// NewRegistryHealthChecker returns a registry health checker
func NewRegistryHealthChecker(service string, registry registry.IRegistry) Checker {
	return &registryHealthChecker{service, registry}
}

type brokerHealthChecker struct {
	service string
	admin   messagebroker.Admin
}

func (b *brokerHealthChecker) checkHealth() (bool, error) {
	return b.admin.IsHealthy()
}

func (b *brokerHealthChecker) name() string {
	return fmt.Sprintf("broker:%v", b.service)
}

// NewBrokerHealthChecker returns a broker health checker
func NewBrokerHealthChecker(service string, admin messagebroker.Admin) Checker {
	return &brokerHealthChecker{service, admin}
}
