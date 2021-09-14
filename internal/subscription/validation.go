package subscription

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/razorpay/metro/internal/credentials"
	"net/url"
	"regexp"
	"strings"

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
	pushConfigPath       = "push_config"
	ackDeadlineSecPath   = "ack_deadline_seconds"
	retryConfigPath      = "retry_config"
	deadLetterConfigPath = "dead_letter_config"
)

// Config validations related values
const (
	// MinAckDeadlineSeconds minimum value for ack deadline in seconds
	MinAckDeadlineSeconds = 10
	// MaxAckDeadlineSeconds maximum value for ack deadline in seconds
	MaxAckDeadlineSeconds = 600
	// DefaultAckDealineSeconds default value for ack deadline in seconds if not specified or 0
	DefaultAckDealineSeconds = 10

	// MinBackOffSeconds minimum supported backoff for retry
	MinBackOffSeconds = 0
	// MaxBackOffSeconds maximum supported backoff for retry
	MaxBackOffSeconds = 3600
	// DefaultMinBackOffSeconds default minimum backoff for retry
	DefaultMinBackOffSeconds = 10
	// DefaultMaxBackoffSeconds default maximums backoff for retry
	DefaultMaxBackoffSeconds = 600

	// MinDeliveryAttempts minimum delivery attempts before deadlettring
	MinDeliveryAttempts = 1
	// MaxDeliveryAttempts maximum delivery attempts before deadlettring
	MaxDeliveryAttempts = 100
	// DefaultDeliveryAttempts sDefault delivery attempts before deadlettring
	DefaultDeliveryAttempts = 5
)

var (
	// ErrInvalidMinBackoff ...
	ErrInvalidMinBackoff = errors.New(fmt.Sprintf("min backoff should be greater than %v seconds", MinBackOffSeconds))
	// ErrInvalidMaxBackoff ...
	ErrInvalidMaxBackoff = errors.New(fmt.Sprintf("max backoff should be less than %v seconds", MaxBackOffSeconds))
	// ErrInvalidMinAndMaxBackoff ...
	ErrInvalidMinAndMaxBackoff = errors.New("min backoff should be less or equal to max backoff")
	// ErrInvalidMaxDeliveryAttempt ...
	ErrInvalidMaxDeliveryAttempt = errors.New(fmt.Sprintf("max delivery attempt should be between %v and %v", MinDeliveryAttempts, MaxDeliveryAttempts))
	// ErrInvalidAckDeadline ...
	ErrInvalidAckDeadline = errors.New(fmt.Sprintf("ack deadline should be between %v and %v", MinAckDeadlineSeconds, MaxAckDeadlineSeconds))
	// ErrInvalidPushEndpointUsername ...
	ErrInvalidPushEndpointUsername = errors.New(fmt.Sprintf("ack deadline should be between %v and %v", MinAckDeadlineSeconds, MaxAckDeadlineSeconds))
	// ErrInvalidPushEndpointPassword ...
	ErrInvalidPushEndpointPassword = errors.New(fmt.Sprintf("ack deadline should be between %v and %v", MinAckDeadlineSeconds, MaxAckDeadlineSeconds))
)

func init() {
	// https://github.com/googleapis/googleapis/blob/69697504d9eba1d064820c3085b4750767be6d08/google/pubsub/v1/pubsub.proto#L636
	// Note: check for project ID would happen while creating the project, hence not enforcing it here
	subscriptionNameRegex = regexp.MustCompile("projects/(.*)/subscriptions/([A-Za-z][A-Za-z0-9-_.~+%]{2,254})$")
}

// GetValidatedModelForCreate validates an incoming proto request and returns the model for create requests
func GetValidatedModelForCreate(ctx context.Context, req *metrov1.Subscription) (*Model, error) {
	m := &Model{}

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

	// get validated pushconfig details
	m.PushConfig, err = getValidatedPushConfig(ctx, req.GetPushConfig())
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "Invalid [subscriptions] push config, %s", err.Error())
	}

	// validate AckDeadline and update model
	m.AckDeadlineSeconds, err = getValidatedAckDeadline(ctx, req.AckDeadlineSeconds)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "Invalid [subscriptions] ack deadline, %s", err.Error())
	}

	m.RetryPolicy, err = getValidatedRetryConfig(ctx, req.RetryPolicy)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "Invalid [subscriptions] retry policy, %s", err.Error())
	}

	m.DeadLetterPolicy, err = getValidatedDeadLetterPolicy(ctx, req.DeadLetterPolicy)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "Invalid [subscriptions] deadletter policy, %s", err.Error())
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

	for _, path := range req.UpdateMask.Paths {
		switch path {
		// valid paths
		case pushConfigPath, ackDeadlineSecPath, retryConfigPath, deadLetterConfigPath:
			continue
		default:
			return merror.Newf(merror.InvalidArgument, "invalid update_mask provided. '%s' is not a known update_mask", path)
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

func validateTopicName(_ context.Context, name string) (string, error) {
	if strings.HasSuffix(name, topic.RetryTopicSuffix) {
		err := fmt.Errorf("subscription topic name cannot end with " + topic.RetryTopicSuffix)
		return "", err
	}

	return name, nil
}

func getValidatedPushConfig(_ context.Context, config *metrov1.PushConfig) (*PushConfig, error) {
	if config != nil {
		// validate the endpoint
		urlEndpoint := config.PushEndpoint
		_, err := url.ParseRequestURI(urlEndpoint)
		if err != nil {
			return nil, err
		}

		pushAttr := config.GetAttributes()
		var creds *credentials.Model

		// check creds and encrypt if present
		if pushAttr != nil {
			var username, password string
			if u, ok := pushAttr[attributeUsername]; ok {
				if strings.Trim(u, " ") == "" {
					return nil, ErrInvalidPushEndpointUsername
				}
				username = u
			}

			if p, ok := pushAttr[attributePassword]; ok {
				if strings.Trim(p, " ") == "" {
					return nil, ErrInvalidPushEndpointPassword
				}
				password = p
			}

			delete(pushAttr, attributeUsername)
			delete(pushAttr, attributePassword)

			// set credentials only if both needed values were sent
			if username != "" && password != "" {
				creds = credentials.NewCredential(username, password)
			}
		}

		return &PushConfig{
			PushEndpoint: urlEndpoint,
			Attributes:   pushAttr,
			Credentials:  creds,
		}, nil
	}

	return nil, nil
}

func getValidatedAckDeadline(ctx context.Context, ackDeadlineSeconds int32) (int32, error) {
	if ackDeadlineSeconds == 0 {
		return DefaultAckDealineSeconds, nil
	}

	if ackDeadlineSeconds < MinAckDeadlineSeconds {
		return 0, ErrInvalidAckDeadline
	}

	if ackDeadlineSeconds > MaxAckDeadlineSeconds {
		return 0, ErrInvalidAckDeadline
	}

	return ackDeadlineSeconds, nil
}

func getValidatedRetryConfig(ctx context.Context, retryPolicy *metrov1.RetryPolicy) (*RetryPolicy, error) {
	if retryPolicy == nil {
		return &RetryPolicy{
			MinimumBackoff: DefaultMinBackOffSeconds,
			MaximumBackoff: DefaultMaxBackoffSeconds,
		}, nil
	}

	// Note that input backoff is of type durationpb.Duration. below we are using only seconds part
	// of the struct, for now we are ignoring the nanoseconds part since metro supports only integer
	// values for backoff
	if retryPolicy.MinimumBackoff.Seconds > retryPolicy.MaximumBackoff.Seconds {
		return nil, ErrInvalidMinAndMaxBackoff
	}

	if retryPolicy.MinimumBackoff.Seconds < MinBackOffSeconds {
		return nil, ErrInvalidMinBackoff
	}

	if retryPolicy.MaximumBackoff.Seconds > MaxBackOffSeconds {
		return nil, ErrInvalidMaxBackoff
	}

	return &RetryPolicy{
		MinimumBackoff: uint(retryPolicy.MinimumBackoff.Seconds),
		MaximumBackoff: uint(retryPolicy.MaximumBackoff.Seconds),
	}, nil
}

func getValidatedDeadLetterPolicy(ctx context.Context, dlpolicy *metrov1.DeadLetterPolicy) (*DeadLetterPolicy, error) {
	// defaultDeadLetterTopic := topic.GetTopicName(p, m.ExtractedSubscriptionName+topic.DeadLetterTopicSuffix)
	defaultDeadLetterTopic := ""

	if dlpolicy == nil {
		return &DeadLetterPolicy{
			MaxDeliveryAttempts: DefaultDeliveryAttempts,
			DeadLetterTopic:     defaultDeadLetterTopic,
		}, nil
	}

	if dlpolicy.MaxDeliveryAttempts < MinDeliveryAttempts {
		return nil, ErrInvalidMaxDeliveryAttempt
	}

	if dlpolicy.MaxDeliveryAttempts > MaxDeliveryAttempts {
		return nil, ErrInvalidMaxDeliveryAttempt
	}

	// TODO: check and validate if dltopic is present in input, validate topic is valid topic
	// and use topic from input instead
	return &DeadLetterPolicy{
		MaxDeliveryAttempts: dlpolicy.MaxDeliveryAttempts,
		DeadLetterTopic:     defaultDeadLetterTopic,
	}, nil
}

func extractSubscriptionMetaAndValidate(_ context.Context, name string) (projectID string, subscriptionName string, err error) {
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
