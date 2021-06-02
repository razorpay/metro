package project

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/razorpay/metro/internal/auth"

	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

var idRegex *regexp.Regexp

func init() {
	var err error
	idRegex, err = regexp.Compile("([a-z][a-z0-9-]{5,29})$")
	if err != nil {
		panic(err)
	}
}

// GetValidatedModelForCreate validates an incoming proto request and returns a project model for create requests
func GetValidatedModelForCreate(ctx context.Context, req *metrov1.Project) (*Model, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name cannot be empty")
	}
	return getValidatedModel(ctx, req)
}

// GetValidatedModelForDelete validates an incoming proto request and returns a project model for delete requests
func GetValidatedModelForDelete(ctx context.Context, req *metrov1.Project) (*Model, error) {
	return getValidatedModel(ctx, req)
}

func getValidatedModel(ctx context.Context, req *metrov1.Project) (*Model, error) {
	if req.ProjectId == "" {
		return nil, fmt.Errorf("projectId cannot be empty")
	}
	if !isValidProjectID(ctx, req.ProjectId) {
		return nil, fmt.Errorf("not a valid projectID")
	}
	return fromProto(req), nil
}

// fromProto creates and returns a Model from proto message
func fromProto(proto *metrov1.Project) *Model {
	m := &Model{}
	m.Name = proto.GetName()
	m.ProjectID = proto.GetProjectId()
	m.Labels = proto.GetLabels()
	m.AppAuth = make(map[string]*auth.Auth)
	return m
}

func isValidProjectID(ctx context.Context, projectID string) bool {
	if strings.HasSuffix(projectID, "-") {
		logger.Ctx(ctx).Error("projectID cannot end with a trailing hyphen")
		return false
	}
	return idRegex.MatchString(projectID)
}
