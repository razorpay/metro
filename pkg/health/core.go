package health

import (
	"context"
	"fmt"
	"sync"
)

// Core holds business logic and/or orchestrator of other things in the package.
type Core struct {
	isHealthy bool
	mutex     sync.Mutex
}

// NewCore creates Core.
func NewCore(_ context.Context) *Core {
	return &Core{
		isHealthy: true,
	}
}

// RunHealthCheck runs various server checks and returns true if all individual components are working fine.
// Todo: Fix server check response per https://tools.ietf.org/id/draft-inadarei-api-health-check-01.html :)
func (c *Core) RunHealthCheck(ctx context.Context) (bool, error) {
	if !c.isHealthy {
		return false, fmt.Errorf("server marked unhealthy")
	}

	var err error

	// Checks the DB connection exists and is alive by executing a select query.
	// err = c.repo.Alive(ctx)
	if err != nil {
		//logger.Ctx(ctx).Errorw("failed to execute select query on db connection", "error", err)
	}
	isDbAlive := err == nil

	return isDbAlive, err
}

// MarkUnhealthy marks the server as unhealthy for health check to return negative
func (c *Core) MarkUnhealthy() {
	c.mutex.Lock()
	c.isHealthy = false
	c.mutex.Unlock()
}
