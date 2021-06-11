package subscription

import (
	"context"
	"encoding/json"
	"github.com/opentracing/opentracing-go"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/pkg/registry"
)

// IRepo interface over database repository
type IRepo interface {
	common.IRepo
	List(ctx context.Context, prefix string) ([]common.IModel, error)
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

// List returns a slice of subscriptions matching prefix
func (r *Repo) List(ctx context.Context, prefix string) ([]common.IModel, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriptionRepository.List")
	defer span.Finish()

	pairs, err := r.Registry.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	models := []common.IModel{}
	for _, pair := range pairs {
		var m Model
		err = json.Unmarshal(pair.Value, &m)
		if err != nil {
			return nil, err
		}
		models = append(models, &m)
	}
	return models, nil
}
