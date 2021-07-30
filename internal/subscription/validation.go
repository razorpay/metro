package subscription

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/topic"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

var subscriptionNameRegex *regexp.Regexp

const (
	// used as keys in the subscription push config attributes
	attributeUsername = "username"
	attributePassword = "password"
)

const (
	// List of patchable attributes(proto)
	pushConfigPath     = "push_config"
	ackDeadlineSecPath = "ack_deadline_seconds"
)

func init() {
	var err error
	// https://github.com/googleapis/googleapis/blob/69697504d9eba1d064820c3085b4750767be6d08/google/pubsub/v1/pubsub.proto#L636
	// Note: check for project ID would happen while creating the project, hence not enforcing it here
	subscriptionNameRegex, err = regexp.Compile("projects/(.*)/subscriptions/([A-Za-z][A-Za-z0-9-_.~+%]{2,254})$")
	if err != nil {
		panic(err)
	}
}

// GetValidatedModelForCreate validates an incoming proto request and returns the model for create requests
func GetValidatedModelForCreate(ctx context.Context, req *metrov1.Subscription) (*Model, error) {
	m, err := getValidatedModel(ctx, req)
	if err != nil {
		return nil, err
	}
	p, t, err := topic.ExtractTopicMetaAndValidate(ctx, req.Topic)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "Invalid [topic] name: (name=%s)", req.Topic)
	}

	m.ExtractedTopicName = t
	m.ExtractedTopicProjectID = p
	m.AckDeadlineSeconds = req.GetAckDeadlineSeconds()

	m.AckDeadlineSeconds = req.AckDeadlineSeconds

	// get validated pushconfig details
	urlEndpoint, err := validatePushConfig(ctx, req.GetPushConfig())
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "Invalid [subscriptions] push config: (url=%s)", urlEndpoint)
	}

	m.PushConfig = &PushConfig{
		PushEndpoint: urlEndpoint,
		Attributes:   req.PushConfig.GetAttributes(),
	}

	m.DeadLetterPolicy = &DeadLetterPolicy{
		DeadLetterTopic:     topic.GetTopicName(p, m.ExtractedSubscriptionName+topic.DeadLetterTopicSuffix),
		MaxDeliveryAttempts: req.DeadLetterPolicy.GetMaxDeliveryAttempts(),
	}

	// set push auth
	if req.GetPushConfig() != nil && req.GetPushConfig().GetAttributes() != nil {
		pushAttr := req.GetPushConfig().GetAttributes()

		var username, password string
		if u, ok := pushAttr[attributeUsername]; ok {
			if strings.Trim(u, " ") == "" {
				return nil, merror.New(merror.InvalidArgument, "Invalid [Username] for push endpoint")
			}
			username = u
		}

		if p, ok := pushAttr[attributePassword]; ok {
			if strings.Trim(p, " ") == "" {
				return nil, merror.New(merror.InvalidArgument, "Invalid [Password] for push endpoint")
			}
			password = p
		}

		// set credentials only if both needed values were sent
		if username != "" && password != "" {
			m.PushConfig.Credentials = credentials.NewCredential(username, password)
		}
	}
	return m, nil
}

// GetValidatedModelForDelete validates an incoming proto request and returns the model for delete requests
func GetValidatedModelForDelete(ctx context.Context, req *metrov1.Subscription) (*Model, error) {
	return getValidatedModel(ctx, req)
}

// GetValidatedModelForUpdate - validates the subscription model for update operation and returns the parsed model
func GetValidatedModelForUpdate(ctx context.Context, req *metrov1.Subscription) (*Model, error) {
	return GetValidatedModelForCreate(ctx, req)
}

// ValidateUpdateSubscriptionRequest - Validates the update subscription request
func ValidateUpdateSubscriptionRequest(ctx context.Context, req *metrov1.UpdateSubscriptionRequest) error {
	req.UpdateMask.Normalize()
	if !req.UpdateMask.IsValid(req.Subscription) {
		err := merror.Newf(merror.InvalidArgument, "invalid update mask provided. Valid values are: %s, %s", pushConfigPath, ackDeadlineSecPath)
		return err
	}
	for _, path := range req.UpdateMask.Paths {
		if path != pushConfigPath && path != ackDeadlineSecPath {
			err := merror.Newf(merror.InvalidArgument, "invalid update_mask provided. '%s' is not a known update_mask", path)
			return err
		}
	}
	return nil
}

func getValidatedModel(ctx context.Context, req *metrov1.Subscription) (*Model, error) {
	// validate and extract the subscription fields from the name
	p, s, err := extractSubscriptionMetaAndValidate(ctx, req.GetName())
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "Invalid [subscriptions] name: (name=%s)", req.Name)
	}

	// get validated topic details
	topicName, err := validateTopicName(ctx, req.GetTopic())
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "Invalid [subscriptions] topic: (topic=%s)", req.GetTopic())
	}

	m := &Model{
		Name:                           req.GetName(),
		Topic:                          topicName,
		Labels:                         req.GetLabels(),
		ExtractedSubscriptionName:      s,
		ExtractedSubscriptionProjectID: p,
	}

	return m, nil
}

func validateTopicName(ctx context.Context, name string) (string, error) {
	if strings.HasSuffix(name, topic.RetryTopicSuffix) {
		err := fmt.Errorf("subscription topic name cannot end with " + topic.RetryTopicSuffix)
		return "", err
	}

	return name, nil
}

func validatePushConfig(ctx context.Context, config *metrov1.PushConfig) (string, error) {
	if config != nil {
		urlEndpoint := config.PushEndpoint
		_, err := url.ParseRequestURI(urlEndpoint)
		if err != nil {
			return "", err
		}

		return urlEndpoint, nil
	}
	return "", nil
}

func extractSubscriptionMetaAndValidate(ctx context.Context, name string) (projectID string, subscriptionName string, err error) {
	match := subscriptionNameRegex.FindStringSubmatch(name)
	if len(match) != 3 {
		err = fmt.Errorf("invalid subscription name")
		return "", "", err
	}
	projectID = subscriptionNameRegex.FindStringSubmatch(name)[1]
	subscriptionName = subscriptionNameRegex.FindStringSubmatch(name)[2]
	if strings.HasPrefix(subscriptionName, "goog") {
		err = fmt.Errorf("subscription name cannot start with goog")
		return "", "", err
	}

	return projectID, subscriptionName, nil
}
