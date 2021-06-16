package health

import (
	"context"
	"fmt"
	"time"

	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
)

type registryHealthChecker struct {
	service  string
	registry registry.IRegistry
}

func (r *registryHealthChecker) checkHealth(ctx context.Context) (bool, error) {
	return r.registry.IsAlive(ctx)
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

func (b *brokerHealthChecker) checkHealth(ctx context.Context) (bool, error) {
	newCtx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*1))
	defer cancel()
	return b.admin.IsHealthy(newCtx)
}

func (b *brokerHealthChecker) name() string {
	return fmt.Sprintf("broker:%v", b.service)
}

// NewBrokerHealthChecker returns a broker health checker
func NewBrokerHealthChecker(service string, admin messagebroker.Admin) Checker {
	return &brokerHealthChecker{service, admin}
}
