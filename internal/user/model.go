package user

import (
	"github.com/razorpay/metro/pkg/errors"
	"github.com/razorpay/metro/pkg/spine"
)

var (
	AttributeStatus = "status"
)

// User defines the user model struct
type User struct {
	spine.SoftDeletableModel
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name"`
	Status     string `json:"status"`
	ApprovedAt int32  `json:"approved_at" sql:"DEFAULT:null"`
}

func (u *User) TableName() string {
	return "users"
}

func (u *User) EntityName() string {
	return "user"
}

func (u *User) SetDefaults() errors.IError {
	return nil
}
