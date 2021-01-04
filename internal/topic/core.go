package topic

import (
	"context"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/internal/project"
)

// ICore is an interface over topic core
//go:generate go run -mod=mod github.com/golang/mock/mockgen -build_flags=-mod=mod -destination=mocks/core/mock_core.go -package=mocks . ICore
type ICore interface {
	CreateTopic(ctx context.Context, topic *Model) error
	Exists(ctx context.Context, key string) (bool, error)
}

// Core implements all business logic for project
type Core struct {
	repo        IRepo
	projectCore project.ICore
}

// NewCore returns an instance of Core
func NewCore(repo IRepo, projectCore project.ICore) *Core {
	return &Core{repo, projectCore}
}

// CreateTopic implements topic creation
func (c *Core) CreateTopic(ctx context.Context, m *Model) error {
	if ok, err := c.projectCore.ExistsWithID(ctx, m.ExtractedProjectID); !ok {
		if err != nil {
			return err
		}
		merror.Newf(merror.NotFound, "project not found")
	}
	ok, err := c.Exists(ctx, m.Key())
	if err != nil {
		return err
	}
	if ok {
		return merror.Newf(merror.AlreadyExists, "topic with id %s already exists", m.Name)
	}
	// TODO: Add topic creation in messagebroker
	return c.repo.Create(ctx, m)
}

// Exists checks if the topic exists with a given key
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	ok, err := c.repo.Exists(ctx, key)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error(), "key", key)
		return false, err
	}
	return ok, nil
}
