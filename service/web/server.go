package web

import (
	"context"
	"regexp"

	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/interceptors"
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
	projectIDRegex = regexp.MustCompile(`^projects\/([^\/]+)\/.*`)
}

func authRequest(ctx context.Context, credCore credentials.ICore, fullMethodName string, req interface{}) (context.Context, error) {
	projectID, err := getProjectIDFromRequest(ctx, req)
	if err != nil {
		return ctx, err
	}
	return interceptors.AppAuth(ctx, credCore, projectID)
}

func getProjectIDFromRequest(ctx context.Context, req interface{}) (string, error) {
	switch req.(type) {
	case *metrov1.ListProjectSubscriptionsRequest:
		return req.(*metrov1.ListProjectSubscriptionsRequest).ProjectId, nil
	case *metrov1.ListProjectTopicsRequest:
		return req.(*metrov1.ListProjectTopicsRequest).ProjectId, nil
	default:
		resourceName, err := getResourceNameFromRequest(ctx, req)
		if err != nil {
			return "", err
		}
		return getProjectIDFromResourceName(ctx, resourceName)
	}
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
		u := req.(*metrov1.UpdateSubscriptionRequest)
		if u.Subscription != nil {
			return u.Subscription.Name, nil
		}
		logger.Ctx(ctx).Errorw("received update subscription without subscription", "request", u)
		return "", unknownResourceError
	case *metrov1.AcknowledgeRequest:
		return req.(*metrov1.AcknowledgeRequest).Subscription, nil
	case *metrov1.PullRequest:
		return req.(*metrov1.PullRequest).Subscription, nil
	case *metrov1.DeleteSubscriptionRequest:
		return req.(*metrov1.DeleteSubscriptionRequest).Subscription, nil
	case *metrov1.ModifyAckDeadlineRequest:
		return req.(*metrov1.ModifyAckDeadlineRequest).Subscription, nil
	case *metrov1.ListTopicSubscriptionsRequest:
		return req.(*metrov1.ListTopicSubscriptionsRequest).Topic, nil
	default:
		logger.Ctx(ctx).Infof("unknown request type: %v", t)
		return "", unknownResourceError
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
		return "", invalidResourceNameError
	}
	return matches[1], nil
}
