package project

import (
	"fmt"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// Validate an incoming proto request
func Validate(req *metrov1.Project) error {
	if req.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	if req.ProjectId == "" {
		return fmt.Errorf("projectId cannot be empty")
	}
	return nil
}
