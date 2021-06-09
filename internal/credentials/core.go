package credentials

import (
	"context"
	"time"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/pkg/logger"
)

// ICore is an interface over credential core
type ICore interface {
	Create(ctx context.Context, credential *Model) error
	Exists(ctx context.Context, key string) (bool, error)
	Delete(ctx context.Context, m *Model) error
	Get(ctx context.Context, projectID, username string) (*Model, error)
}

// Core implements all business logic for a credential
type Core struct {
	repo        IRepo
	projectCore project.ICore
}

// NewCore returns an instance of Core
func NewCore(repo IRepo, projectCore project.ICore) *Core {
	return &Core{repo, projectCore}
}

// Create creates a credential for a given project
func (c *Core) Create(ctx context.Context, m *Model) error {

	credentialOperationCount.WithLabelValues(env, "Create").Inc()

	startTime := time.Now()
	defer func() {
		credentialOperationTimeTaken.WithLabelValues(env, "Create").Observe(time.Now().Sub(startTime).Seconds())
	}()

	if ok, err := c.projectCore.ExistsWithID(ctx, m.ProjectID); !ok {
		if err != nil {
			return err
		}
		logger.Ctx(ctx).Errorw("credential project not found", "name", m.ProjectID)
		return merror.New(merror.NotFound, "project not found")
	}
	ok, err := c.Exists(ctx, m.Key())
	if err != nil {
		return err
	}
	if ok {
		return merror.Newf(merror.AlreadyExists, "credential %s already exists", m.Key())
	}

	return c.repo.Save(ctx, m)
}

// Exists checks if credential exists
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	credentialOperationCount.WithLabelValues(env, "Exists").Inc()

	startTime := time.Now()
	defer func() {
		credentialOperationTimeTaken.WithLabelValues(env, "Exists").Observe(time.Now().Sub(startTime).Seconds())
	}()

	ok, err := c.repo.Exists(ctx, key)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error(), "credential", key)
		return false, err
	}
	return ok, nil
}

// Delete deletes the given credential
func (c *Core) Delete(ctx context.Context, m *Model) error {
	credentialOperationCount.WithLabelValues(env, "Delete").Inc()

	startTime := time.Now()
	defer func() {
		credentialOperationTimeTaken.WithLabelValues(env, "Delete").Observe(time.Now().Sub(startTime).Seconds())
	}()

	err := c.repo.Delete(ctx, m)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error())
		return err
	}
	return nil
}

// Get checks if credential exists
func (c *Core) Get(ctx context.Context, projectID, username string) (*Model, error) {
	credentialOperationCount.WithLabelValues(env, "Get").Inc()

	startTime := time.Now()
	defer func() {
		credentialOperationTimeTaken.WithLabelValues(env, "Get").Observe(time.Now().Sub(startTime).Seconds())
	}()

	prefix := common.GetBasePrefix() + Prefix + projectID + "/" + username
	model := &Model{}
	err := c.repo.Get(ctx, prefix, model)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error(), "prefix", prefix)
		return nil, err
	}
	return model, nil
}
