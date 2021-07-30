package subscription

import (
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
		}
		if m.PushConfig.Credentials != nil {
			proto.PushConfig.Attributes = map[string]string{}
			proto.PushConfig.Attributes[attributeUsername] = m.PushConfig.Credentials.GetUsername()
			proto.PushConfig.Attributes[attributePassword] = m.PushConfig.Credentials.GetPassword()
		}
	}
	return proto
}
