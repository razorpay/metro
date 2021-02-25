package subscription

import (
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/pkg/registry"
)

// IRepo interface over database repository
type IRepo interface {
	common.IRepo
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
