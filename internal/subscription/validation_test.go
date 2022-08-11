// +build unit

package subscription

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/pkg/httpclient"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	fieldmaskpb "google.golang.org/protobuf/types/known/fieldmaskpb"
)

func Test_extractSubscriptionMetaAndValidate(t *testing.T) {
	ctx := context.Background()
	proj, subs, err := extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/test-sub")
	assert.Nil(t, err)
	assert.Equal(t, "test-project", proj)
	assert.Equal(t, "test-sub", subs)

	// should fail as subscription name contains invalid char
	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/test-sub$")
	assert.NotNil(t, err)

	// should fail as subscription name has invalid format
	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/test-sub")
	assert.NotNil(t, err)

	// should fail as subscription name starts with goog
	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/goog-test-sub")
	assert.NotNil(t, err)

	// should fail as subscription name has invalid length
	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/to")
	assert.NotNil(t, err)
}

func Test_validateSubscriptionName(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		nameInput  string
		err        error
		nameOutput string
		project    string
		id         string
	}{
		{
			nameInput:  "projects/test-project/subscriptions/test-sub",
			err:        nil,
			nameOutput: "projects/test-project/subscriptions/test-sub",
			project:    "test-project",
			id:         "test-sub",
		},
		{
			nameInput:  "projects/test-project/subscriptions/test-sub$",
			err:        errors.New(fmt.Sprintf("Invalid [subscriptions] name: (name=%s)", "projects/test-project/subscriptions/test-sub$")),
			nameOutput: "",
			project:    "",
			id:         "",
		},
		{
			nameInput:  "projects/test-project/test-sub",
			err:        errors.New("Invalid [subscriptions] name: (name=projects/test-project/test-sub)"),
			nameOutput: "",
			project:    "",
			id:         "",
		},
		{
			nameInput:  "projects/test-project/subscriptions/goog-test-sub",
			err:        ErrInvalidSubscriptionName,
			nameOutput: "",
			project:    "",
			id:         "",
		},
		{
			nameInput:  "projects/test-project/subscriptions/to",
			err:        errors.New("Invalid [subscriptions] name: (name=projects/test-project/subscriptions/to)"),
			nameOutput: "",
			project:    "",
			id:         "",
		},
	}

	for _, test := range tests {
		m := &Model{}
		err := validateSubscriptionName(ctx, m, &metrov1.Subscription{Name: test.nameInput})
		if err != nil {
			assert.Equal(t, test.err.Error(), err.Error())
		} else {
			assert.Equal(t, test.err, err)
		}
		assert.Equal(t, test.nameOutput, m.Name)
		assert.Equal(t, test.project, m.ExtractedSubscriptionProjectID)
		assert.Equal(t, test.id, m.ExtractedSubscriptionName)
	}
}

func Test_validateTopicName(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		topicInput   string
		err          error
		topicOutput  string
		topicProject string
		topicID      string
	}{
		{
			"projects/test-project/topics/test-topic",
			nil,
			"projects/test-project/topics/test-topic",
			"test-project",
			"test-topic",
		},
		{
			"projects/test-project/topics/test-topic-retry",
			ErrInvalidTopicName,
			"",
			"",
			"",
		},
	}

	for _, test := range tests {
		m := &Model{}
		err := validateTopicName(ctx, m, &metrov1.Subscription{Topic: test.topicInput})
		assert.Equal(t, test.err, err)
		assert.Equal(t, test.topicOutput, m.Topic)
		assert.Equal(t, test.topicProject, m.ExtractedTopicProjectID)
		assert.Equal(t, test.topicID, m.ExtractedTopicName)
	}
}

func Test_validatePushConfig(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		inputConfig  *metrov1.PushConfig
		err          error
		outputConfig *PushConfig
	}{
		{
			&metrov1.PushConfig{
				PushEndpoint: "https://www.razorpay.com",
			},
			nil,
			&PushConfig{
				PushEndpoint: "https://www.razorpay.com",
			},
		},
		{
			nil,
			nil,
			nil,
		},
		{
			&metrov1.PushConfig{
				PushEndpoint: "abcd",
			},
			ErrInvalidPushEndpointURL,
			nil,
		},
		{
			&metrov1.PushConfig{
				PushEndpoint: "https://www.razorpay.com",
				AuthenticationMethod: &metrov1.PushConfig_BasicAuth_{
					BasicAuth: &metrov1.PushConfig_BasicAuth{
						Username: "",
					},
				},
			},
			ErrInvalidPushEndpointUsername,
			nil,
		},
		{
			&metrov1.PushConfig{
				PushEndpoint: "https://www.razorpay.com",
				AuthenticationMethod: &metrov1.PushConfig_BasicAuth_{
					BasicAuth: &metrov1.PushConfig_BasicAuth{
						Username: "abcd",
						Password: "",
					},
				},
			},
			ErrInvalidPushEndpointPassword,
			nil,
		},
		{
			&metrov1.PushConfig{
				PushEndpoint: "https://www.razorpay.com",
				AuthenticationMethod: &metrov1.PushConfig_BasicAuth_{
					BasicAuth: &metrov1.PushConfig_BasicAuth{
						Username: "username",
						Password: "password",
					},
				},
			},
			nil,
			&PushConfig{
				PushEndpoint: "https://www.razorpay.com",
				Credentials:  credentials.NewCredential("username", "password"),
			},
		},
	}

	for _, test := range tests {
		m := &Model{}
		err := validatePushConfig(ctx, m, &metrov1.Subscription{PushConfig: test.inputConfig})
		assert.Equal(t, test.err, err)
		assert.Equal(t, test.outputConfig, m.PushConfig)
	}
}

func Test_validateSubscriptionRequestInvalidPath(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"abcd"},
		},
	}
	err := ValidateUpdateSubscriptionRequest(ctx, req)
	assert.NotNil(t, err)
}

func Test_validateSubscriptionRequestUneditablePath(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"topic"},
		},
	}
	err := ValidateUpdateSubscriptionRequest(ctx, req)
	assert.NotNil(t, err)
}

func Test_validateSubscriptionRequest(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"push_config"},
		},
	}
	err := ValidateUpdateSubscriptionRequest(ctx, req)
	assert.Nil(t, err)
}

func Test_validateRetryConfig(t *testing.T) {
	ctx := context.Background()

	type test struct {
		inputPolicy  *metrov1.RetryPolicy
		err          error
		outputPolicy *RetryPolicy
	}

	tests := []test{
		{
			nil,
			nil,
			&RetryPolicy{
				MinimumBackoff: 10,
				MaximumBackoff: 600,
			},
		},
		{
			&metrov1.RetryPolicy{
				MinimumBackoff: &durationpb.Duration{Seconds: 30},
				MaximumBackoff: &durationpb.Duration{Seconds: 600},
			},
			nil,
			&RetryPolicy{
				MinimumBackoff: 30,
				MaximumBackoff: 600,
			},
		},
		{
			&metrov1.RetryPolicy{
				MinimumBackoff: &durationpb.Duration{Seconds: 100},
				MaximumBackoff: &durationpb.Duration{Seconds: 30},
			},
			ErrInvalidMinAndMaxBackoff,
			nil,
		},
		{
			&metrov1.RetryPolicy{
				MinimumBackoff: &durationpb.Duration{Seconds: -1},
				MaximumBackoff: &durationpb.Duration{Seconds: 30},
			},
			ErrInvalidMinBackoff,
			nil,
		},
		{
			&metrov1.RetryPolicy{
				MinimumBackoff: &durationpb.Duration{Seconds: 1},
				MaximumBackoff: &durationpb.Duration{Seconds: 4000},
			},
			ErrInvalidMaxBackoff,
			nil,
		},
	}

	for _, test := range tests {
		m := &Model{}
		err := validateRetryPolicy(ctx, m, &metrov1.Subscription{RetryPolicy: test.inputPolicy})
		assert.Equal(t, test.err, err)
		assert.Equal(t, test.outputPolicy, m.RetryPolicy)
	}
}

func Test_validateDeadletterConfig(t *testing.T) {
	ctx := context.Background()

	p := "pid"
	s := "sid"
	name := fmt.Sprintf("projects/%s/subscriptions/%s", p, s)

	type test struct {
		inputPolicy  *metrov1.DeadLetterPolicy
		err          error
		outputPolicy *DeadLetterPolicy
	}

	tests := []test{
		{
			nil,
			nil,
			&DeadLetterPolicy{
				MaxDeliveryAttempts: 5,
				DeadLetterTopic:     "projects/pid/topics/sid-dlq",
			},
		},
		{
			&metrov1.DeadLetterPolicy{
				MaxDeliveryAttempts: 101,
				DeadLetterTopic:     "projects/pid/topics/sid-dl1",
			},
			ErrInvalidMaxDeliveryAttempt,
			nil,
		},
		{
			&metrov1.DeadLetterPolicy{
				MaxDeliveryAttempts: 1,
				DeadLetterTopic:     "projects/pid/topics/sid-dl1",
			},
			nil,
			&DeadLetterPolicy{
				MaxDeliveryAttempts: 1,
				DeadLetterTopic:     "projects/pid/topics/sid-dlq",
			},
		},
		{
			&metrov1.DeadLetterPolicy{
				MaxDeliveryAttempts: 1,
			},
			nil,
			&DeadLetterPolicy{
				MaxDeliveryAttempts: 1,
				DeadLetterTopic:     "projects/pid/topics/sid-dlq",
			},
		},
		{
			&metrov1.DeadLetterPolicy{
				MaxDeliveryAttempts: 0,
			},
			ErrInvalidMaxDeliveryAttempt,
			nil,
		},
	}

	for _, test := range tests {
		m := &Model{
			Name:                           name,
			ExtractedSubscriptionProjectID: p,
			ExtractedSubscriptionName:      s,
		}

		err := validateDeadLetterPolicy(ctx, m, &metrov1.Subscription{DeadLetterPolicy: test.inputPolicy})
		assert.Equal(t, test.err, err)
		assert.Equal(t, test.outputPolicy, m.DeadLetterPolicy)
	}
}

func Test_validateFilterExpression(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		filterExpression string
		err              error
	}{
		{
			filterExpression: "attributes:x",
			err:              nil,
		},
		{
			filterExpression: "attributes.x = \"com\"",
			err:              nil,
		},
		{
			filterExpression: "attributes.x != \"com\"",
			err:              nil,
		},
		{
			filterExpression: "NOT attributes.x = \"com\"",
			err:              nil,
		},
		{
			filterExpression: "hasPrefix(attributes.x, \"a\")",
			err:              nil,
		},
		{
			filterExpression: "hasSuffix(attributes.x, \"a\")",
			err:              errors.New("1:1: unexpected token \"hasSuffix\" (expected (BasicExpression | (\"(\" Condition \")\")))"),
		},
		{
			filterExpression: "attributes:domain AND NOT hasPrefix(attributes.domain, \"co\") AND attributes.domain != \"com\" AND (attributes:x AND NOT hasPrefix(attributes.x, \"co\") AND attributes.x != \"com\")",
			err:              nil,
		},
		{
			filterExpression: "attributes:domain AND NOT hasPrefix(attributes.domain, \"co\") AND attributes.domain != \"com\" AND (attributes:x OR NOT hasPrefix(attributes.x, \"co\") OR attributes.x != \"com\")",
			err:              nil,
		},
		{
			filterExpression: "attributes:domain AND NOT hasPrefix(attributes.domain, \"co\") OR attributes.domain != \"com\" AND (attributes:x AND NOT hasPrefix(attributes.x, \"co\") AND attributes.x != \"com\"",
			err:              errors.New("1:62: unexpected token \"OR\""),
		},
		{
			filterExpression: "attributes:domain OR ",
			err:              errors.New("1:19: sub-expression (\"OR\" Term)+ must match at least once"),
		},
	}
	for _, test := range tests {
		m := &Model{}
		err := validateFilterExpression(ctx, m, &metrov1.Subscription{Filter: test.filterExpression})
		if err != nil {
			assert.Equal(t, test.err.Error(), err.Error())
		} else {
			assert.Equal(t, test.err, err)
		}
		assert.Equal(t, test.filterExpression, m.FilterExpression)
	}
}

func Test_validatePushEndpoint(t *testing.T) {
	ctx := context.Background()
	client := httpclient.NewClient(&httpclient.Config{ConnectTimeoutMS: 100000})

	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	type testCase struct {
		inputConfig *metrov1.PushConfig
		err         error
		reachable   bool
	}

	tests := [2]testCase{
		{
			&metrov1.PushConfig{
				PushEndpoint: "https://randomReachableUrl.com",
			},
			nil,
			true,
		},
		{
			&metrov1.PushConfig{
				PushEndpoint: "https://randomNotReachableUrl.com",
			},
			ErrPushEndpointNotReachable,
			false,
		},
	}

	for _, test := range tests {
		if test.reachable {
			httpmock.RegisterResponder("GET", test.inputConfig.PushEndpoint,
				httpmock.NewStringResponder(200, "This endpoint is reachable"))
		}
		err := validatePushEndpoint(ctx, &metrov1.Subscription{PushConfig: test.inputConfig}, client)
		assert.Equal(t, test.err, err)
	}
}
