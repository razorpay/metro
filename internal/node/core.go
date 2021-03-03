package node

import (
	"context"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/pkg/logger"
)

// ICore is an interface over node core
type ICore interface {
	CreateNode(ctx context.Context, m *Model) error
	Exists(ctx context.Context, key string) (bool, error)
	ExistsWithID(ctx context.Context, id string) (bool, error)
	DeleteNode(ctx context.Context, m *Model) error
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

// ExistsWithID to check if the node exists with the ID
func (c *Core) ExistsWithID(ctx context.Context, id string) (bool, error) {
	return c.Exists(ctx, common.BasePrefix+Prefix+id)
}

// List gets all nodes
func (c *Core) ListKeys(ctx context.Context) ([]string, error) {
	return c.repo.ListKeys(ctx, Prefix)
}

// DeleteNode deletes a node and all resources in it
func (c *Core) DeleteNode(ctx context.Context, m *Model) error {
	if ok, err := c.Exists(ctx, m.Key()); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "node not found %s", m.ID)
	}
	return c.repo.DeleteTree(ctx, m)
}
