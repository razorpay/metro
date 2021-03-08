package subscription

import (
	"context"
	"encoding/json"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/pkg/registry"
)

// IRepo interface over database repository
type IRepo interface {
	common.IRepo
	List(ctx context.Context, prefix string) ([]Model, error)
}

// Repo implements various repository methods
type Repo struct {
	common.BaseRepo
}

// NewRepo returns IRepo
func NewRepo(registry registry.IRegistry) IRepo {
	return &Repo{
		common.BaseRepo{Registry: registry},
	}
}

// List returns a slice of NodeBindings matching prefix
func (r *Repo) List(ctx context.Context, prefix string) ([]Model, error) {
	prefix = Prefix + prefix
	pairs, err := r.Registry.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	models := []Model{}
	for _, pair := range pairs {
		var m Model
		err = json.Unmarshal(pair.Value, &m)
		if err != nil {
			return nil, err
		}
		models = append(models, m)
	}
	return models, nil
}
