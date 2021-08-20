package credentials

import (
	"context"
	"fmt"
	"regexp"

	"github.com/razorpay/metro/internal/project"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	usernameRegex *regexp.Regexp
	// UnauthenticatedError error returned when credential validation fails
	UnauthenticatedError = status.Error(codes.Unauthenticated, "Unauthenticated")
)

func init() {
	usernameRegex = regexp.MustCompile("([a-z][a-z0-9-]{5,29})__[a-zA-Z0-9]{6}")
}

// GetValidatedModelForCreate validates an incoming proto request and returns the model for create requests
func GetValidatedModelForCreate(ctx context.Context, req *metrov1.ProjectCredentials) (*Model, error) {
	if req.ProjectId == "" {
		return nil, fmt.Errorf("projectId cannot be empty")
	}
	if !project.IsValidProjectID(ctx, req.ProjectId) {
		return nil, fmt.Errorf("not a valid projectID")
	}
	return fromProto(req), nil
}

// fromProto creates and returns a Model from proto message
func fromProto(proto *metrov1.ProjectCredentials) *Model {
	m := &Model{}
	m.ProjectID = proto.GetProjectId()
	m.Username = newUsername(proto.GetProjectId())
	m.Password = newPassword()
	return m
}

// IsValidUsername checks if the username is of the expected format
func IsValidUsername(username string) bool {
	return usernameRegex.MatchString(username)
}
