package web

import (
	"context"
	"regexp"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

var projectIDRegex *regexp.Regexp

func init() {
	// Regex to capture project id from resource names
	// Resources are in the form /projects/<project-id>/<resource-type>/<resource-id>
	projectIDRegex = regexp.MustCompile(`^projects\/([^\/]+)\/.*`)
}

func getProjectIDFromRequest(ctx context.Context, req interface{}) (string, error) {
	resourceName, err := getResourceNameFromRequest(ctx, req)
	if err != nil {
		return "", err
	}
	return getProjectIDFromResourceName(ctx, resourceName)
}

func getResourceNameFromRequest(ctx context.Context, req interface{}) (string, error) {
	switch t := req.(type) {
	case *metrov1.PublishRequest:
		return req.(*metrov1.PublishRequest).Topic, nil
	case *metrov1.Topic:
		return req.(*metrov1.Topic).Name, nil
	case *metrov1.DeleteTopicRequest:
		return req.(*metrov1.DeleteTopicRequest).Topic, nil
	case *metrov1.Subscription:
		return req.(*metrov1.Subscription).Name, nil
	case *metrov1.UpdateSubscriptionRequest:
		return req.(*metrov1.UpdateSubscriptionRequest).Subscription.Name, nil
	case *metrov1.AcknowledgeRequest:
		return req.(*metrov1.AcknowledgeRequest).Subscription, nil
	case *metrov1.PullRequest:
		return req.(*metrov1.PullRequest).Subscription, nil
	case *metrov1.DeleteSubscriptionRequest:
		return req.(*metrov1.DeleteSubscriptionRequest).Subscription, nil
	case *metrov1.ModifyAckDeadlineRequest:
		return req.(*metrov1.ModifyAckDeadlineRequest).Subscription, nil
	default:
		logger.Ctx(ctx).Infof("unknown request type: %v", t)
		err := merror.New(merror.Unimplemented, "unknown resource type")
		return "", err
	}
}

// getProjectIDFromResourceName - Fetches the project id from resource name using regex capturing group
// Example: projects/project001/subscriptions/subscription001 -> project001
//          projects/project001/topics/topic001               -> project001
//          topics/topic001                                   -> invalid
func getProjectIDFromResourceName(ctx context.Context, resourceName string) (string, error) {
	matches := projectIDRegex.FindStringSubmatch(resourceName)
	if len(matches) < 2 {
		logger.Ctx(ctx).Errorw("could not extract project id from resource name", "resource name", resourceName)
		return "", merror.New(merror.InvalidArgument, "Resource name is invalid")
	}
	return matches[1], nil
}
