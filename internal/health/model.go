package health

import (
	"context"
	"fmt"
	"time"

	"github.com/razorpay/metro/pkg/cache"
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
)

type registryHealthChecker struct {
	registry registry.IRegistry
}

func (r *registryHealthChecker) checkHealth(ctx context.Context) (bool, error) {
	return r.registry.IsAlive(ctx)
}

func (r *registryHealthChecker) name() string {
	return fmt.Sprintf("registry:%T", r)
}

// NewRegistryHealthChecker returns a registry health checker
func NewRegistryHealthChecker(registry registry.IRegistry) Checker {
	return &registryHealthChecker{registry}
}

type cacheHealthChecker struct {
	cache cache.ICache
}

func (c *cacheHealthChecker) checkHealth(ctx context.Context) (bool, error) {
	return c.cache.IsAlive(ctx)
}

func (c *cacheHealthChecker) name() string {
	return fmt.Sprintf("cache:%T", c)
}

// NewCacheHealthChecker returns a cache health checker
func NewCacheHealthChecker(cache cache.ICache) Checker {
	return &cacheHealthChecker{cache}
}

type brokerHealthChecker struct {
	admin messagebroker.Admin
}

func (b *brokerHealthChecker) checkHealth(ctx context.Context) (bool, error) {
	newCtx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*1))
	defer cancel()
	return b.admin.IsHealthy(newCtx)
}

func (b *brokerHealthChecker) name() string {
	return fmt.Sprintf("broker:%T", b)
}

// NewBrokerHealthChecker returns a broker health checker
func NewBrokerHealthChecker(admin messagebroker.Admin) Checker {
	return &brokerHealthChecker{admin}
}
