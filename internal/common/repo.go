package common

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// IRepo is an interface over common repo functionality
type IRepo interface {
	Save(ctx context.Context, m IModel) error
	Acquire(ctx context.Context, m IModel, sessionID string) error
	Exists(ctx context.Context, key string) (bool, error)
	Delete(ctx context.Context, m IModel) error
	DeleteTree(ctx context.Context, key string) error
	Get(ctx context.Context, key string, m IModel) error
	ListKeys(ctx context.Context, prefix string) ([]string, error)
}

// BaseRepo implements base repo methods
type BaseRepo struct {
	Registry registry.IRegistry
}

// Save puts the model in the registry
func (r BaseRepo) Save(ctx context.Context, m IModel) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "BaseRepository.Save")
	defer span.Finish()

	b, err := json.Marshal(m)
	if err != nil {
		logger.Ctx(ctx).Error("error in json marshal", "msg", err.Error())
		return err
	}
	return r.Registry.Put(ctx, m.Key(), b)
}

// Acquire puts the model in the registry with a attempt to lock using sessionID
func (r BaseRepo) Acquire(ctx context.Context, m IModel, sessionID string) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "BaseRepository.Acquire")
	defer span.Finish()

	b, err := json.Marshal(m)
	if err != nil {
		logger.Ctx(ctx).Error("error in json marshal", "msg", err.Error())
		return err
	}
	acquired, err := r.Registry.Acquire(ctx, sessionID, m.Key(), b)
	if err != nil {
		return err
	}

	if !acquired {
		return errors.New("failed to acquire node key")
	}

	return nil
}

// Exists to check if the key exists in the registry
func (r BaseRepo) Exists(ctx context.Context, key string) (bool, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "BaseRepository.Exists")
	defer span.Finish()

	return r.Registry.Exists(ctx, key)
}

// Delete deletes all keys under a model key
func (r BaseRepo) Delete(ctx context.Context, m IModel) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "BaseRepository.Delete")
	defer span.Finish()

	return r.Registry.DeleteTree(ctx, m.Key())
}

// DeleteTree deletes all keys under a prefix
func (r BaseRepo) DeleteTree(ctx context.Context, key string) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "BaseRepository.DeleteTree")
	defer span.Finish()

	return r.Registry.DeleteTree(ctx, key)
}

// Get populates m with value corresponding to key
func (r BaseRepo) Get(ctx context.Context, key string, m IModel) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "BaseRepository.Get")
	defer span.Finish()

	b, err := r.Registry.Get(ctx, key)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, m)
	if err != nil {
		return err
	}
	return nil
}

// ListKeys populates keys with key list corresponding to prefix
func (r BaseRepo) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "BaseRepository.ListKeys")
	defer span.Finish()

	keys, err := r.Registry.ListKeys(ctx, prefix)
	if err != nil {
		return nil, err
	}

	return keys, nil
}
