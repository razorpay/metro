package auth

import (
	"github.com/razorpay/metro/pkg/utils"
)

// IAuth define the auth model contract
type IAuth interface {
	GetUsername() string
	GetPassword() string
}

// Auth holds basic auth credentials for push endpoint
type Auth struct {
	Username string `json:"username"`
	Password string `json:"password"`
	// in future, this model can contain some ACL as well
}

// NewAuth returns a new auth model
func NewAuth(username, password string) *Auth {
	a := &Auth{}
	a.Username = username
	// store Password only after encoding it
	a.Password = utils.Encode(password)
	return a
}

// GetUsername returns the auth Username
func (auth *Auth) GetUsername() string {
	return auth.Username
}

// GetPassword returns the auth Password
func (auth *Auth) GetPassword() string {
	// decode before reading Password
	return utils.Decode(auth.Password)
}
