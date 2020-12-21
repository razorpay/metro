package project

import (
	"github.com/razorpay/metro/internal/common"

	"github.com/razorpay/metro/pkg/registry"
)

// IRepo interface over database repository
type IRepo interface {
	Create(m common.Model) error
}

// Repo implements various repository methods
type Repo struct {
	common.Repo
}

// NewRepo returns IRepo
func NewRepo(registry registry.Registry) IRepo {
	return &Repo{
		common.Repo{registry},
	}
}
