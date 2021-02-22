package task

import (
	"context"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
)

// ICore is an interface over task core
//go:generate go run -mod=mod github.com/golang/mock/mockgen -build_flags=-mod=mod -destination=mocks/core/mock_core.go -package=mocks . ICore
type ICore interface {
	CreateTask(ctx context.Context, m *Model) error
	Exists(ctx context.Context, key string) (bool, error)
	DeleteTask(ctx context.Context, m *Model) error
}

// Core implements all business logic for a task
type Core struct {
	repo      IRepo
	topicCore topic.ICore
}

// NewCore returns an instance of Core
func NewCore(repo IRepo, topicCore topic.ICore) *Core {
	return &Core{repo, topicCore}
}

// CreateTask creates a task for a given topic
func (c *Core) CreateTask(ctx context.Context, m *Model) error {
	// check if task exists
	ok, err := c.Exists(ctx, m.Key())
	if err != nil {
		return err
	}
	if ok {
		return merror.Newf(merror.AlreadyExists, "task with id %s already exists", m.ID)
	}
	if ok, err = c.topicCore.ExistsWithName(ctx, m.Topic); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "topic with name %s not found", m.Topic)
	}
	return c.repo.Create(ctx, m)
}

// Exists checks if task exists for a given key
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	ok, err := c.repo.Exists(ctx, key)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error(), "key", key)
		return false, err
	}
	return ok, nil
}

// DeleteTask deletes a task
func (c *Core) DeleteTask(ctx context.Context, m *Model) error {
	if ok, err := c.Exists(ctx, m.Key()); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "Task does not exist")
	}
	return c.repo.DeleteTree(ctx, m)
}
