package nodebinding

import (
	"context"
	"time"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/pkg/logger"
)

// ICore is an interface over nodebinding core
type ICore interface {
	CreateNodeBinding(ctx context.Context, m *Model) error
	Exists(ctx context.Context, key string) (bool, error)
	DeleteNodeBinding(ctx context.Context, key string, m *Model) error
	ListKeys(ctx context.Context, prefix string) ([]string, error)
	List(ctx context.Context, prefix string) ([]*Model, error)
	TriggerNodeBindingRefresh(ctx context.Context) error
}

// Core implements all business logic for nodebinding
type Core struct {
	repo IRepo
}

// NewCore returns an instance of Core
func NewCore(repo IRepo) ICore {
	return &Core{repo}
}

// TriggerNodeBindingRefresh enables devs to refresh nodebindings manually
func (c *Core) TriggerNodeBindingRefresh(ctx context.Context) error {
	var err error
	startTime := time.Now()
	defer func() {
		nodeBindingOperationTimeTaken.WithLabelValues(env, "TriggerNodeBindingRefresh").Observe(time.Now().Sub(startTime).Seconds())
	}()

	m := &Model{}
	err = c.repo.DeleteTree(ctx, m.Prefix())
	if err != nil {
		logger.Ctx(ctx).Errorw("Failed to delete nodebinding tree", "error", err.Error())
		return err
	}
	err = c.repo.DeleteTree(ctx, common.GetBasePrefix()+"leader/")
	if err != nil {
		logger.Ctx(ctx).Errorw("Failed to remove leader lock", "error", err.Error())
		return err
	}
	logger.Ctx(ctx).Infow("successfully cleared all nodebindings")
	return err
}

// CreateNodeBinding creates a new nodebinding
func (c *Core) CreateNodeBinding(ctx context.Context, m *Model) error {
	nodeBindingOperationCount.WithLabelValues(env, "CreateNodeBinding").Inc()

	startTime := time.Now()
	defer func() {
		nodeBindingOperationTimeTaken.WithLabelValues(env, "CreateNodeBinding").Observe(time.Now().Sub(startTime).Seconds())
	}()

	ok, err := c.Exists(ctx, m.Key())
	if err != nil {
		return err
	}
	if ok {
		return merror.Newf(merror.AlreadyExists, "nodebinding with id %s already exists", m.Key())
	}
	return c.repo.Save(ctx, m)
}

// Exists to check if the nodebinding exists with fully qualified consul key
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	nodeBindingOperationCount.WithLabelValues(env, "Exists").Inc()

	startTime := time.Now()
	defer func() {
		nodeBindingOperationTimeTaken.WithLabelValues(env, "Exists").Observe(time.Now().Sub(startTime).Seconds())
	}()

	ok, err := c.repo.Exists(ctx, key)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error())
		return false, err
	}
	return ok, nil
}

// ListKeys gets all nodebinding keys
func (c *Core) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	nodeBindingOperationCount.WithLabelValues(env, "ListKeys").Inc()

	startTime := time.Now()
	defer func() {
		nodeBindingOperationTimeTaken.WithLabelValues(env, "ListKeys").Observe(time.Now().Sub(startTime).Seconds())
	}()

	prefix = common.GetBasePrefix() + prefix
	return c.repo.ListKeys(ctx, prefix)
}

// List gets all nodebinding keys starting with given prefix
func (c *Core) List(ctx context.Context, prefix string) ([]*Model, error) {
	nodeBindingOperationCount.WithLabelValues(env, "List").Inc()

	startTime := time.Now()
	defer func() {
		nodeBindingOperationTimeTaken.WithLabelValues(env, "List").Observe(time.Now().Sub(startTime).Seconds())
	}()

	prefix = common.GetBasePrefix() + prefix

	out := []*Model{}
	ret, err := c.repo.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	for _, obj := range ret {
		out = append(out, obj.(*Model))
	}
	return out, nil
}

// DeleteNodeBinding deletes a nodebinding and all resources in it
func (c *Core) DeleteNodeBinding(ctx context.Context, key string, m *Model) error {
	nodeBindingOperationCount.WithLabelValues(env, "DeleteNodeBinding").Inc()

	startTime := time.Now()
	defer func() {
		nodeBindingOperationTimeTaken.WithLabelValues(env, "DeleteNodeBinding").Observe(time.Since(startTime).Seconds())
	}()

	if ok, err := c.Exists(ctx, key); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "nodebinding not found %s", key)
	}
	return c.repo.Delete(ctx, m)
}
