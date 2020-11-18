package errors

import "sync"

const (
	Default                              = "default"
	BadRequestError                      = "bad_request_error"
	BadRequestValidationFailureException = "bad_request_validation_failure"
)

// defaultErrorMap map of internal code to public error
var defaultErrorMap = map[string]IPublic{
	// TODO: discuss proper message and code
	Default: &Public{
		Code:        "server_error",
		Description: "something bad happened",
	},
	BadRequestError: &Public{
		Code:        "BAD_REQUEST_ERROR",
		Description: "Bad request",
	},

	BadRequestValidationFailureException: &Public{
		Code:        "BAD_REQUEST_VALIDATION_FAILURE",
		Description: "Validation Failure",
	},
}

// errorMap holds the error map of internal code to public error
type errorMap struct {
	sync.Mutex
	mappingList map[string]IPublic
}

var mapping = &errorMap{
	mappingList: defaultErrorMap,
}

// Get will return the public error for the given internal code
// if the code does not exist then it'll return the default error
func (em *errorMap) Get(code string) IPublic {
	if detail, ok := em.mappingList[code]; ok {
		return detail
	}

	return em.mappingList[Default]
}

// Register will update the internal code to public details map
// *note: if the same code is passed, it'll replace the value
func Register(code string, detail IPublic) {
	mapping.Lock()
	defer mapping.Unlock()

	mapping.mappingList[code] = detail
}
