package user

import (
	"context"

	"github.com/razorpay/metro/pkg/errors"
	"github.com/razorpay/metro/pkg/spine"
)

type IRepo interface {
	Create(ctx context.Context, receiver spine.IModel) errors.IError
	FindByID(ctx context.Context, receiver spine.IModel, id string) errors.IError
	FindMany(ctx context.Context, receivers interface{}, condition map[string]interface{}) errors.IError
	Delete(ctx context.Context, receiver spine.IModel) errors.IError
	Update(ctx context.Context, receiver spine.IModel, attrList ...string) errors.IError
}

/*
type Repo struct {
	spine.Repo
}
func NewRepo(ctx context.Context, db *db.DB) IRepo {
	_ = ctx

	return &Repo{
		Repo: spine.Repo{
			Db: db,
		},
	}
}
*/
