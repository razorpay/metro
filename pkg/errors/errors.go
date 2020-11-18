package errors

import (
	"encoding/json"

	"github.com/razorpay/metro/pkg/errorresponse"
)

type IError interface {
	error

	Class() Class
	Internal() IInternal
	Public() IPublic

	Is(err error) bool
	Wrap(err error) IError
	Unwrap() error
	WithPublic(public IPublic) IError
	WithInternalMetadata(fields map[string]string) IError
	Serialize(withInternal bool) ([]byte, error)
}

// Error struct holds the error details
// this consists of three sections
// 1. class: defines the type of error
// 2. Public: holds the data which can be exposed in Public response
// 3. internal: holds the internal error details for logging and debugging purpose
type Error struct {
	class    Class
	public   *Public
	internal *internal
}

// Error constructs the error message with available data
// this error is specifically for internal consumption
func (e Error) Error() string {
	msg := e.class.Name() + ": " + e.internal.Error()

	return msg
}

// Class returns the error class
func (e Error) Class() Class {
	return e.class
}

// Internal returns the internal error
func (e Error) Internal() IInternal {
	return e.internal
}

// Public returns the Public error
func (e Error) Public() IPublic {
	return e.public
}

// WithPublic updates the non empty data from given input to
// Public error details
func (e Error) WithPublic(public IPublic) IError {
	_ = e.public.
		withStep(public.GetStep()).
		withCode(public.GetCode()).
		withField(public.GetField()).
		withReason(public.GetReason()).
		withSource(public.GetSource()).
		withMetadata(public.GetMetadata()).
		withDescription(public.Error())

	return e
}

// WithInternalMetadata appends the given fields with internal error fields
func (e Error) WithInternalMetadata(fields map[string]string) IError {
	e.internal.withMetadata(fields)
	return e
}

// MarshalJSON implements the json.Marshaller interface, allowing Error
// to be serialized for Public use
func (e Error) MarshalJSON() ([]byte, error) {
	errResp := errorresponse.Error{
		Error: e.public.toErrorResponse(),
	}

	if e.internal != nil {
		errResp.InternalError = e.internal.toErrorResponse()
	}

	return json.Marshal(errResp)
}

// Serialize will marshal the error
// data considered for marshaling depends on the parameter passed
// internal error will be included / excluded based on the value of param
func (e Error) Serialize(withInternal bool) ([]byte, error) {
	if !withInternal {
		return json.Marshal(Error{
			class:  e.class,
			public: e.public,
		})
	}

	return json.Marshal(e)
}

// Wrap holds the cause of the error
func (e Error) Wrap(cause error) IError {
	e.internal.withCause(cause)
	return e
}

// Unwrap will return the cause of error
func (e Error) Unwrap() error {
	return e.internal.Cause()
}

// Is checks if source and destination error are of the same class
func (e Error) Is(err error) bool {
	iErr, ok := err.(IError)
	if !ok {
		return e.IsOfClass(err)
	}

	return e.Class().Name() == iErr.Class().Name()
}

// IsOfClass checks if the error belongs to the class
// where the target type is Class
func (e Error) IsOfClass(err error) bool {
	errClass, ok := err.(Class)
	if !ok {
		return false
	}

	return e.Class().Name() == errClass.Name()
}
