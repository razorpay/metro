package consume_plane

import (
	"context"
	"regexp"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

var projectIDRegex *regexp.Regexp

var (
	unknownResourceError     = merror.New(merror.InvalidArgument, "unknown resource type")
	invalidResourceNameError = merror.New(merror.InvalidArgument, "resource name is invalid")
)

func init() {
	// Regex to capture project id from resource names
	// Resources are in the form /projects/<project-id>/<resource-type>/<resource-id>
	// projectIDRegex = regexp.MustCompile(`^projects\/([^\/]+)\/.*`)
}

func getResourceNameFromRequest(ctx context.Context, req interface{}) (string, error) {
	switch t := req.(type) {
	case *metrov1.AcknowledgeRequest:
		return req.(*metrov1.AcknowledgeRequest).Subscription, nil
	case *metrov1.PullRequest:
		return req.(*metrov1.PullRequest).Subscription, nil
	case *metrov1.ModifyAckDeadlineRequest:
		return req.(*metrov1.ModifyAckDeadlineRequest).Subscription, nil
	default:
		logger.Ctx(ctx).Infof("unknown request type: %v", t)
		return "", unknownResourceError
	}
}
