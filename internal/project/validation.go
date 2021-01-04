package project

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// GetValidatedModel validates an incoming proto request and returns a project model
func GetValidatedModel(ctx context.Context, req *metrov1.Project) (*Model, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name cannot be empty")
	}
	if req.ProjectId == "" {
		return nil, fmt.Errorf("projectId cannot be empty")
	}
	if isValidProjectID(ctx, req.ProjectId) {
		return fromProto(req), nil
	}
	return nil, fmt.Errorf("not a valid projectID")
}

// fromProto creates and returns a Model from proto message
func fromProto(proto *metrov1.Project) *Model {
	m := &Model{}
	m.Name = proto.GetName()
	m.ProjectID = proto.GetProjectId()
	m.Labels = proto.GetLabels()
	return m
}

func isValidProjectID(ctx context.Context, projectID string) bool {
	if strings.HasSuffix(projectID, "-") {
		logger.Ctx(ctx).Error("projectID cannot end with a trailing hyphen")
		return false
	}
	r, err := regexp.Compile("([a-z][a-z0-9-]{5,29})$")
	if err != nil {
		logger.Ctx(ctx).Error(err.Error())
		return false
	}
	return r.MatchString(projectID)
}
