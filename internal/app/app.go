package app

import "os"

// Env holds the current environment
var Env string

func init() {
	Env = os.Getenv("APP_ENV")
	if Env == "" {
		Env = "dev"
	}
}

// GetEnv returns the current environment, prod, dev etc
func GetEnv() string {
	// Fetch env for bootstrapping
	return Env
}

// IsTestMode checks if the current env is a test mode env or not
// TODO find a way to remove the need for this in the future
func IsTestMode() bool {
	if Env == "dev" || Env == "test" || Env == "dev_docker" {
		return true
	}
	return false
}
