package subscription

import (
	"google.golang.org/protobuf/types/known/durationpb"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

//ModelToSubscriptionProtoV1 - Convert internal model into protov1 model
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
			if proto.PushConfig.Attributes == nil {
				proto.PushConfig.Attributes = map[string]string{}
			}
			proto.PushConfig.Attributes[attributeUsername] = m.PushConfig.Credentials.GetUsername()
			proto.PushConfig.Attributes[attributePassword] = m.PushConfig.Credentials.GetPassword()
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

	return proto
}
