package mode

import "os"

var Env string

func init() {
	Env = os.Getenv("EXEC_MODE")
}

// IsTestMode return the execution mode
func IsTestMode() bool {
	if Env == "" || Env == "test" {
		return true
	}
	return false
}
