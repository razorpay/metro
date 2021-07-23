package subscription

import metrov1 "github.com/razorpay/metro/rpc/proto/v1"

func ModelToSubscriptionProtoV1(m *Model) *metrov1.Subscription {
	proto := &metrov1.Subscription{
		Name:               m.Name,
		Topic:              m.Topic,
		Labels:             m.Labels,
		AckDeadlineSeconds: m.AckDeadlineSec,
	}

	if m.IsPush() {
		proto.PushConfig = &metrov1.PushConfig{
			PushEndpoint: m.PushEndpoint,
		}
		if m.Credentials != nil {
			proto.PushConfig.Attributes = map[string]string{}
			proto.PushConfig.Attributes[attributeUsername] = m.Credentials.GetUsername()
			proto.PushConfig.Attributes[attributePassword] = m.Credentials.GetPassword()
		}
	}
	return proto
}
