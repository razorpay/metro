package project

import (
	"context"

	"github.com/razorpay/metro/internal/common"

	"github.com/razorpay/metro/pkg/registry"
)

// IRepo interface over database repository
//go:generate mockgen -destination=mocks/repo/mock_repo.go -package=mocks . IRepo
type IRepo interface {
	Create(ctx context.Context, m common.IModel) error
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
