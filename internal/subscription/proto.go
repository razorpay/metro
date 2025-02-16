package subscription

import (
	"google.golang.org/protobuf/types/known/durationpb"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// ModelToSubscriptionProtoV1 - Convert internal model into protov1 model
func ModelToSubscriptionProtoV1(m *Model) *metrov1.Subscription {
	proto := &metrov1.Subscription{
		Name:               m.Name,
		Topic:              m.Topic,
		Labels:             m.Labels,
		AckDeadlineSeconds: m.AckDeadlineSeconds,
	}

	if m.IsPush() {
		proto.PushConfig = &metrov1.PushConfig{
			PushEndpoint: m.PushConfig.PushEndpoint,
			Attributes:   m.PushConfig.Attributes,
		}
		if m.PushConfig.Credentials != nil {
			proto.PushConfig.AuthenticationMethod = &metrov1.PushConfig_BasicAuth_{
				BasicAuth: &metrov1.PushConfig_BasicAuth{
					Username: m.PushConfig.Credentials.GetUsername(),
					Password: m.PushConfig.Credentials.GetPassword(),
				},
			}
		}
	}

	if m.RetryPolicy != nil {
		proto.RetryPolicy = &metrov1.RetryPolicy{
			MinimumBackoff: &durationpb.Duration{Seconds: int64(m.RetryPolicy.MinimumBackoff)},
			MaximumBackoff: &durationpb.Duration{Seconds: int64(m.RetryPolicy.MaximumBackoff)},
		}
	}

	if m.DeadLetterPolicy != nil {
		proto.DeadLetterPolicy = &metrov1.DeadLetterPolicy{
			MaxDeliveryAttempts: m.DeadLetterPolicy.MaxDeliveryAttempts,
			DeadLetterTopic:     m.DeadLetterPolicy.DeadLetterTopic,
		}
	}

	if m.FilterExpression != "" {
		proto.Filter = m.FilterExpression
	}

	return proto
}
