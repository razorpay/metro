package subscription

import (
	"context"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/pkg/registry"
)

// IRepo interface over database repository
//go:generate go run -mod=mod github.com/golang/mock/mockgen -build_flags=-mod=mod -destination=mocks/repo/mock_repo.go -package=mocks . IRepo
type IRepo interface {
	Create(ctx context.Context, m common.IModel) error
	Exists(ctx context.Context, key string) (bool, error)
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
