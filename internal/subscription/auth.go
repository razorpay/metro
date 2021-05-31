package subscription

import "github.com/razorpay/metro/internal/common"

// Auth holds basic auth credentials for push endpoint
type Auth struct {
	Username string
	Password string
}

// NewAuth returns a new auth model
func NewAuth(username, password string) *Auth {
	a := &Auth{}
	a.Username = username
	// store Password only after encoding it
	a.Password = common.Encode(password)
	return a
}

// GetUsername returns the auth Username
func (auth *Auth) GetUsername() string {
	return auth.Username
}

// GetPassword returns the auth Password
func (auth *Auth) GetPassword() string {
	// decode before reading Password
	return common.Decode(auth.Password)
}
