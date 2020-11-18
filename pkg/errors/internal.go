package errors

import "github.com/razorpay/metro/pkg/errorresponse"

type IInternal interface {
	error

	Code() string
	Metadata() map[string]string
	StackTrace() StackTrace
}

type internal struct {
	code     string
	metadata map[string]string
	cause    error
	stack    *stack
}

// newInternal will create a new instance internal struct
// and fill that with given error code
func newInternal(code string) *internal {
	return &internal{
		code:     code,
		metadata: make(map[string]string),
		stack:    callers(1),
	}
}

// Code will return the error code
func (i *internal) Code() string {
	return i.code
}

// Cause will return the cause of the error
// if there any
func (i *internal) Cause() error {
	return i.cause
}

// Error will give the error as string
// if there is description then returns the code
func (i *internal) Error() string {
	if i.cause == nil {
		return i.code
	}

	return i.cause.Error()
}

// MetaData will give the meta data of the error
func (i *internal) Metadata() map[string]string {
	return i.metadata
}

// StackTrace gives the error stack trance
func (i *internal) StackTrace() StackTrace {
	return i.stack.StackTrace()
}

// toErrorResponse will convert the error into error response format
func (i *internal) toErrorResponse() *errorresponse.Internal {
	return &errorresponse.Internal{
		Code:        i.Code(),
		Description: i.Error(),
		Metadata:    i.Metadata(),
	}
}

// withMetadata appends the meta data given as with available meta details
func (i *internal) withMetadata(fields map[string]string) {
	for key, val := range fields {
		i.metadata[key] = val
	}
}

// withCause will add error description detail
func (i *internal) withCause(cause error) {
	i.cause = cause
}
