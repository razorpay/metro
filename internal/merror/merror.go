package merror

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MError is metro error
type MError struct {
	code    Code
	message string
}

// Error returns the error message
func (e *MError) Error() string {
	return e.message
}

// Code returns the error code
func (e *MError) Code() Code {
	return e.code
}

// ToGRPCError returns the grpc error
func (e *MError) ToGRPCError() error {
	return ToGRPCError(e)
}

// ToGRPCError converts merror to grpc status error
func ToGRPCError(e error) error {
	if merr, ok := e.(*MError); ok {
		return status.Error(internalToGRPCCodeMapping[merr.code], merr.message)
	}
	return status.Error(codes.Unknown, e.Error())
}

// New returns a merror with code and a message
func New(code Code, msg string) *MError {
	return &MError{code: code, message: msg}
}

// Newf returns a merror with code and a formatted message
func Newf(code Code, format string, arg ...interface{}) *MError {
	return &MError{code: code, message: fmt.Sprintf(format, arg...)}
}
