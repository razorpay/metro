package app

import "os"

// GetEnv returns the current environment, prod, dev etc
func GetEnv() string {
	// Fetch env for bootstrapping
	environment := os.Getenv("APP_ENV")
	if environment == "" {
		environment = "dev"
	}

	return environment
}

// IsTestMode return true if the current execution env is test mode
func IsTestMode() bool {
	env := GetEnv()
	if env == "test" || env == "dev" || env == "dev_docker" {
		return true
	}
	return false
}
