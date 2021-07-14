package merror

import "google.golang.org/grpc/codes"

// Code is internal error code type
type Code uint32

const (
	// OK is returned on success.
	OK Code = 0
	// Canceled indicates the operation was canceled (typically by the caller).
	Canceled Code = 1
	// Unknown error errors raised by APIs that do not return enough error information
	// may be converted to this error.
	Unknown Code = 2
	// InvalidArgument indicates client specified an invalid argument.
	InvalidArgument Code = 3
	// DeadlineExceeded means operation expired before completion.
	DeadlineExceeded Code = 4
	// NotFound means some requested entity (e.g., file or directory) was
	// not found.
	// This error code will not be generated by the gRPC framework.
	NotFound Code = 5
	// AlreadyExists means an attempt to create an entity failed because one
	// already exists.
	AlreadyExists Code = 6
	// PermissionDenied indicates the caller does not have permission to
	// execute the specified operation.
	PermissionDenied Code = 7
	// ResourceExhausted indicates some resource has been exhausted, perhaps
	// a per-user quota, or perhaps the entire file system is out of space.
	ResourceExhausted Code = 8
	// FailedPrecondition indicates operation was rejected because the
	// system is not in a state required for the operation's execution.
	// For example, directory to be deleted may be non-empty, an rmdir
	// operation is applied to a non-directory, etc.
	FailedPrecondition Code = 9
	// Aborted indicates the operation was aborted, typically due to a
	// concurrency issue like sequencer check failures, transaction aborts,
	// etc.
	Aborted Code = 10
	// OutOfRange means operation was attempted past the valid range.
	// E.g., seeking or reading past end of file.
	OutOfRange Code = 11
	// Unimplemented indicates operation is not implemented or not
	// supported/enabled in this service.
	Unimplemented Code = 12
	// Internal errors. Means some invariants expected by underlying
	// system has been broken. If you see one of these errors,
	// something is very broken.
	Internal Code = 13
	// Unavailable indicates the service is currently unavailable.
	// This is a most likely a transient condition and may be corrected
	// by retrying with a backoff. Note that it is not always safe to retry
	// non-idempotent operations.
	Unavailable Code = 14
	// DataLoss indicates unrecoverable data loss or corruption.
	DataLoss Code = 15
	// Unauthenticated indicates the request does not have valid
	// authentication credentials for the operation.
	Unauthenticated Code = 16
)

var internalToGRPCCodeMapping map[Code]codes.Code

func init() {
	internalToGRPCCodeMapping = map[Code]codes.Code{
		OK:                 codes.OK,
		Canceled:           codes.Canceled,
		Unknown:            codes.Unknown,
		InvalidArgument:    codes.InvalidArgument,
		DeadlineExceeded:   codes.DeadlineExceeded,
		NotFound:           codes.NotFound,
		AlreadyExists:      codes.AlreadyExists,
		PermissionDenied:   codes.PermissionDenied,
		ResourceExhausted:  codes.ResourceExhausted,
		FailedPrecondition: codes.FailedPrecondition,
		Aborted:            codes.Aborted,
		OutOfRange:         codes.OutOfRange,
		Unimplemented:      codes.Unimplemented,
		Internal:           codes.Internal,
		Unavailable:        codes.Unavailable,
		Unauthenticated:    codes.Unauthenticated,
	}
}
