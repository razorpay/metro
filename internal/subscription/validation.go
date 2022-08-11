package subscription

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/pkg/errors"

	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/httpclient"
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
	retryConfigPath      = "retry_policy"
	deadLetterConfigPath = "dead_letter_policy"
	filterPath           = "filter"
)

// Config validations related values
const (
	// MinAckDeadlineSeconds minimum value for ack deadline in seconds
	MinAckDeadlineSeconds = 10
	// MaxAckDeadlineSeconds maximum value for ack deadline in seconds
	MaxAckDeadlineSeconds = 600
	// DefaultAckDeadlineSeconds default value for ack deadline in seconds if not specified or 0
	DefaultAckDeadlineSeconds = 10

	// MinBackOffSeconds minimum supported backoff for retry
	MinBackOffSeconds = 0
	// MaxBackOffSeconds maximum supported backoff for retry
	MaxBackOffSeconds = 3600
	// DefaultMinBackOffSeconds default minimum backoff for retry
	DefaultMinBackOffSeconds = 10
	// DefaultMaxBackoffSeconds default maximums backoff for retry
	DefaultMaxBackoffSeconds = 600

	// MinDeliveryAttempts minimum delivery attempts before deadlettering
	MinDeliveryAttempts = 1
	// MaxDeliveryAttempts maximum delivery attempts before deadlettering
	MaxDeliveryAttempts = 100
	// DefaultDeliveryAttempts sDefault delivery attempts before deadlettering
	DefaultDeliveryAttempts = 5
	// Timeout to check the push endpoint is reachable or not
	TimeoutInMs = 1000
)

var (
	// ErrInvalidSubscriptionName ...
	ErrInvalidSubscriptionName = errors.New("subscription name cannot start with goog")

	// ErrInvalidTopicName ...
	ErrInvalidTopicName = errors.New("subscription topic name cannot ends with " + topic.RetryTopicSuffix)

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

	// ErrInvalidPushEndpointURL ...
	ErrInvalidPushEndpointURL = errors.New("invalid push_endpoint url")

	// ErrInvalidPushEndpointUsername ...
	ErrInvalidPushEndpointUsername = errors.New("invalid push_endpoint username attribute")

	// ErrInvalidPushEndpointPassword ...
	ErrInvalidPushEndpointPassword = errors.New("invalid push_endpoint password attribute")

	// ErrPushEndpointNotReachable ...
	ErrPushEndpointNotReachable = errors.New("push_endpoint not reachable")
)

func init() {
	// https://github.com/googleapis/googleapis/blob/69697504d9eba1d064820c3085b4750767be6d08/google/pubsub/v1/pubsub.proto#L636
	// Note: check for project ID would happen while creating the project, hence not enforcing it here
	subscriptionNameRegex = regexp.MustCompile("projects/(.*)/subscriptions/([A-Za-z][A-Za-z0-9-_.~+%]{2,254})$")
}

// GetValidatedModelForCreate validates an incoming proto request and returns the model for create requests
func GetValidatedModelForCreate(ctx context.Context, req *metrov1.Subscription) (*Model, error) {
	m := &Model{}

	err := validateSubscriptionName(ctx, m, req)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}

	err = validateTopicName(ctx, m, req)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}

	// validate Labels
	err = validateLabels(ctx, m, req)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}
	// get validated pushConfig details
	err = validatePushConfig(ctx, m, req)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}

	// validate AckDeadline and update model
	err = validateAckDeadline(ctx, m, req)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}

	err = validateRetryPolicy(ctx, m, req)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}

	err = validateDeadLetterPolicy(ctx, m, req)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}

	// validate Filter Expression
	err = validateFilterExpression(ctx, m, req)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}

	// check push endpoint is reachable
	err = validatePushEndpoint(ctx, req, getHTTPClient(TimeoutInMs))
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}

	m.EnableMessageOrdering = req.EnableMessageOrdering

	return m, nil
}

// GetValidatedModelForDelete validates an incoming proto request and returns the model for delete requests
func GetValidatedModelForDelete(ctx context.Context, req *metrov1.Subscription) (*Model, error) {
	m := &Model{}
	err := validateSubscriptionName(ctx, m, req)
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, err.Error())
	}
	return m, nil
}

// GetValidatedModelForUpdate - validates the subscription model for update operation and returns the parsed model
func GetValidatedModelForUpdate(ctx context.Context, req *metrov1.Subscription) (*Model, error) {
	return GetValidatedModelForCreate(ctx, req)
}

// ValidateUpdateSubscriptionRequest - Validates the update subscription request
func ValidateUpdateSubscriptionRequest(_ context.Context, req *metrov1.UpdateSubscriptionRequest) error {
	req.UpdateMask.Normalize()

	for _, path := range req.UpdateMask.Paths {
		switch path {
		// valid paths
		case pushConfigPath, ackDeadlineSecPath, retryConfigPath, deadLetterConfigPath, filterPath:
			continue
		default:
			return merror.Newf(merror.InvalidArgument, "invalid update_mask provided. '%s' is not a known update_mask", path)
		}
	}

	return nil
}

func validateSubscriptionName(ctx context.Context, m *Model, req *metrov1.Subscription) error {
	// validate and extract the subscription fields from the name
	p, s, err := extractSubscriptionMetaAndValidate(ctx, req.GetName())
	if err != nil {
		return err
	}

	m.Name = req.GetName()
	m.ExtractedSubscriptionProjectID = p
	m.ExtractedSubscriptionName = s

	return nil
}

func validateFilterExpression(ctx context.Context, m *Model, req *metrov1.Subscription) error {
	if req.Filter != "" {
		m.FilterExpression = req.Filter

		// validate that the filter expression is in the expected format.
		// To validate, we convert it into a GO Struct with the defined Grammar in Struct Tags
		_, err := m.GetFilterExpressionAsStruct()
		if err != nil {
			return err
		}
	}
	return nil
}

func validateTopicName(ctx context.Context, m *Model, req *metrov1.Subscription) error {
	name := req.Topic
	if strings.HasSuffix(name, topic.RetryTopicSuffix) {
		return ErrInvalidTopicName
	}

	p, t, err := topic.ExtractTopicMetaAndValidate(ctx, name)
	if err != nil {
		return err
	}

	m.Topic = name
	m.ExtractedTopicProjectID = p
	m.ExtractedTopicName = t

	return nil
}

func validateLabels(_ context.Context, m *Model, req *metrov1.Subscription) error {
	// TODO: add validations on labels
	labels := req.GetLabels()
	if labels != nil {
		m.Labels = labels
	} else {
		m.Labels = make(map[string]string)
	}
	return nil
}

func validatePushConfig(_ context.Context, m *Model, req *metrov1.Subscription) error {
	config := req.GetPushConfig()

	if config != nil {
		// validate the endpoint
		urlEndpoint := config.PushEndpoint
		_, err := url.ParseRequestURI(urlEndpoint)
		if err != nil {
			return ErrInvalidPushEndpointURL
		}

		var creds *credentials.Model

		// check creds and encrypt if present
		if config.GetAuthenticationMethod() != nil {
			// check if basic auth creds are present
			if basicAuthCreds := config.GetBasicAuth(); basicAuthCreds != nil {
				var username, password string

				u := basicAuthCreds.GetUsername()
				if strings.Trim(u, " ") == "" {
					return ErrInvalidPushEndpointUsername
				}
				username = u

				p := basicAuthCreds.GetPassword()
				if strings.Trim(p, " ") == "" {
					return ErrInvalidPushEndpointPassword
				}
				password = p

				// set credentials only if both needed values were sent
				if username != "" && password != "" {
					creds = credentials.NewCredential(username, password)
				}
			}
		}

		m.PushConfig = &PushConfig{
			PushEndpoint: urlEndpoint,
			Attributes:   config.GetAttributes(),
			Credentials:  creds,
		}
	}

	return nil
}

func validateAckDeadline(_ context.Context, m *Model, req *metrov1.Subscription) error {
	ackDeadlineSeconds := req.AckDeadlineSeconds

	if ackDeadlineSeconds == 0 {
		m.AckDeadlineSeconds = DefaultAckDeadlineSeconds
		return nil
	}

	if ackDeadlineSeconds < MinAckDeadlineSeconds {
		return ErrInvalidAckDeadline
	}

	if ackDeadlineSeconds > MaxAckDeadlineSeconds {
		return ErrInvalidAckDeadline
	}

	m.AckDeadlineSeconds = ackDeadlineSeconds
	return nil
}

func validateRetryPolicy(_ context.Context, m *Model, req *metrov1.Subscription) error {
	retryPolicy := req.GetRetryPolicy()

	if retryPolicy == nil {
		m.RetryPolicy = &RetryPolicy{
			MinimumBackoff: DefaultMinBackOffSeconds,
			MaximumBackoff: DefaultMaxBackoffSeconds,
		}
		return nil
	}

	// Note that input backoff is of type durationpb.Duration. below we are using only seconds part
	// of the struct, for now we are ignoring the nanoseconds part since metro supports only integer
	// values for backoff
	if retryPolicy.MinimumBackoff.Seconds > retryPolicy.MaximumBackoff.Seconds {
		return ErrInvalidMinAndMaxBackoff
	}

	if retryPolicy.MinimumBackoff.Seconds < MinBackOffSeconds {
		return ErrInvalidMinBackoff
	}

	if retryPolicy.MaximumBackoff.Seconds > MaxBackOffSeconds {
		return ErrInvalidMaxBackoff
	}

	m.RetryPolicy = &RetryPolicy{
		MinimumBackoff: uint(retryPolicy.MinimumBackoff.Seconds),
		MaximumBackoff: uint(retryPolicy.MaximumBackoff.Seconds),
	}

	return nil
}

func validateDeadLetterPolicy(_ context.Context, m *Model, req *metrov1.Subscription) error {
	defaultDeadLetterTopic := topic.GetTopicName(m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName+topic.DeadLetterTopicSuffix)

	dlpolicy := req.GetDeadLetterPolicy()
	if dlpolicy == nil {
		m.DeadLetterPolicy = &DeadLetterPolicy{
			MaxDeliveryAttempts: DefaultDeliveryAttempts,
			DeadLetterTopic:     defaultDeadLetterTopic,
		}

		return nil
	}

	if dlpolicy.MaxDeliveryAttempts < MinDeliveryAttempts {
		return ErrInvalidMaxDeliveryAttempt
	}

	if dlpolicy.MaxDeliveryAttempts > MaxDeliveryAttempts {
		return ErrInvalidMaxDeliveryAttempt
	}

	// TODO: check and validate if dltopic is present in input, validate topic is valid topic
	// and use topic from input instead
	m.DeadLetterPolicy = &DeadLetterPolicy{
		MaxDeliveryAttempts: dlpolicy.MaxDeliveryAttempts,
		DeadLetterTopic:     defaultDeadLetterTopic,
	}

	return nil
}

func validatePushEndpoint(_ context.Context, req *metrov1.Subscription, client *http.Client) error {
	if req.GetPushConfig() == nil {
		return ErrPushEndpointNotReachable
	}

	resp, err := client.Get(req.GetPushConfig().PushEndpoint)

	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		return ErrPushEndpointNotReachable
	}
	return nil
}

func extractSubscriptionMetaAndValidate(_ context.Context, name string) (projectID string, subscriptionName string, err error) {
	tokens := subscriptionNameRegex.FindStringSubmatch(name)
	if len(tokens) != 3 {
		return "", "", errors.New(fmt.Sprintf("Invalid [subscriptions] name: (name=%s)", name))
	}

	projectID = tokens[1]
	subscriptionName = tokens[2]
	if strings.HasPrefix(subscriptionName, "goog") || strings.HasSuffix(subscriptionName, topic.SubscriptionSuffix) {
		return "", "", ErrInvalidSubscriptionName
	}

	return projectID, subscriptionName, nil
}

func getHTTPClient(timeout int) *http.Client {
	config := &httpclient.Config{ConnectTimeoutMS: timeout}
	return httpclient.NewClient(config)
}
