package merror

import "google.golang.org/grpc/codes"

// Code is internal error code type
type Code int

const (
	// InvalidArgument ...
	InvalidArgument Code = iota + 1
	// AlreadyExists ...
	AlreadyExists
	// NotFound ...
	NotFound
	// Unknown ...
	Unknown
)

var internalToGRPCCodeMapping map[Code]codes.Code

func init() {
	internalToGRPCCodeMapping = map[Code]codes.Code{
		InvalidArgument: codes.InvalidArgument,
		AlreadyExists:   codes.AlreadyExists,
		Unknown:         codes.Unknown,
		NotFound:        codes.NotFound,
	}
}
