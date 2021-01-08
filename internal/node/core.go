package node

import (
	"context"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/pkg/logger"
)

// ICore is an interface over node core
//go:generate go run -mod=mod github.com/golang/mock/mockgen -build_flags=-mod=mod -destination=mocks/core/mock_core.go -package=mocks . ICore
type ICore interface {
	CreateNode(ctx context.Context, m *Model) error
	Exists(ctx context.Context, key string) (bool, error)
}

// Core implements all business logic for node
type Core struct {
	repo IRepo
}

// NewCore returns an instance of Core
func NewCore(repo IRepo) *Core {
	return &Core{repo}
}

// CreateNode creates a new node
func (c *Core) CreateNode(ctx context.Context, m *Model) error {
	ok, err := c.Exists(ctx, m.Key())
	if err != nil {
		return err
	}
	if ok {
		return merror.Newf(merror.AlreadyExists, "node with id %s already exists", m.ID)
	}
	return c.repo.Create(ctx, m)
}

// Exists to check if the node exists with fully qualified consul key
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	logger.Ctx(ctx).Infow("exists query on node", "key", key)
	ok, err := c.repo.Exists(ctx, key)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error())
		return false, err
	}
	return ok, nil
}
