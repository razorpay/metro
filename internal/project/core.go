package project

import (
	"context"
	"time"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/pkg/logger"
)

// ICore is an interface over project core
type ICore interface {
	CreateProject(ctx context.Context, m *Model) error
	Exists(ctx context.Context, key string) (bool, error)
	ExistsWithID(ctx context.Context, id string) (bool, error)
	DeleteProject(ctx context.Context, m *Model) error
}

// Core implements all business logic for project
type Core struct {
	repo IRepo
}

// NewCore returns an instance of Core
func NewCore(repo IRepo) *Core {
	return &Core{repo}
}

// CreateProject creates a new project
func (c *Core) CreateProject(ctx context.Context, m *Model) error {
	projectOperationCount.WithLabelValues(env, "CreateProject").Inc()

	startTime := time.Now()
	defer func() {
		projectOperationTimeTaken.WithLabelValues(env, "CreateProject").Observe(time.Now().Sub(startTime).Seconds())
	}()

	ok, err := c.Exists(ctx, m.Key())
	if err != nil {
		return err
	}
	if ok {
		return merror.Newf(merror.AlreadyExists, "project with id %s already exists", m.ProjectID)
	}
	return c.repo.Save(ctx, m)
}

// Exists to check if the project exists with fully qualified consul key
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	projectOperationCount.WithLabelValues(env, "Exists").Inc()

	startTime := time.Now()
	defer func() {
		projectOperationTimeTaken.WithLabelValues(env, "Exists").Observe(time.Now().Sub(startTime).Seconds())
	}()

	logger.Ctx(ctx).Infow("exists query on project", "key", key)
	ok, err := c.repo.Exists(ctx, key)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error())
		return false, err
	}
	return ok, nil
}

// ExistsWithID to check if the project exists with the projectID
func (c *Core) ExistsWithID(ctx context.Context, id string) (bool, error) {
	projectOperationCount.WithLabelValues(env, "ExistsWithID").Inc()

	startTime := time.Now()
	defer func() {
		projectOperationTimeTaken.WithLabelValues(env, "ExistsWithID").Observe(time.Now().Sub(startTime).Seconds())
	}()

	return c.Exists(ctx, common.GetBasePrefix()+Prefix+id)
}

// DeleteProject deletes a project and all resources in it
func (c *Core) DeleteProject(ctx context.Context, m *Model) error {
	projectOperationCount.WithLabelValues(env, "DeleteProject").Inc()

	startTime := time.Now()
	defer func() {
		projectOperationTimeTaken.WithLabelValues(env, "DeleteProject").Observe(time.Now().Sub(startTime).Seconds())
	}()

	if ok, err := c.Exists(ctx, m.Key()); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "project not found %s", m.ProjectID)
	}

	return c.repo.Delete(ctx, m)
}
