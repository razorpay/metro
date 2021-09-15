// +build unit

package subscription

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	fieldmaskpb "google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/razorpay/metro/internal/credentials"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
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
				Attributes: map[string]string{
					"username": "",
				},
			},
			ErrInvalidPushEndpointUsername,
			nil,
		},
		{
			&metrov1.PushConfig{
				PushEndpoint: "https://www.razorpay.com",
				Attributes: map[string]string{
					"username": "username",
					"password": "",
				},
			},
			ErrInvalidPushEndpointPassword,
			nil,
		},
		{
			&metrov1.PushConfig{
				PushEndpoint: "https://www.razorpay.com",
				Attributes: map[string]string{
					"username": "username",
					"password": "password",
				},
			},
			nil,
			&PushConfig{
				PushEndpoint: "https://www.razorpay.com",
				Attributes:   map[string]string{},
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

		err := validatedDeadLetterPolicy(ctx, m, &metrov1.Subscription{DeadLetterPolicy: test.inputPolicy})
		assert.Equal(t, test.err, err)
		assert.Equal(t, test.outputPolicy, m.DeadLetterPolicy)
	}
}
