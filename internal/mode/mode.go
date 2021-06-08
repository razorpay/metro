package mode

import "os"

// Mode return the execution mode
var Mode string

func init() {
	Mode = os.Getenv("EXEC_MODE")
}

// IsTestMode return the execution mode
func IsTestMode() bool {
	if Mode == "" || Mode == "test" {
		return true
	}
	return false
}
