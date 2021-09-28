// +build unit

package subscription

import (
	"testing"

	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/pkg/encryption"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func Test_ModelToSubscriptionProtoV1(t *testing.T) {
	pwd, _ := encryption.EncryptAsHexString([]byte("password"))
	subscription := &Model{
		Name:  "projects/project123/subscriptions/subscription123",
		Topic: "projects/project123/topics/topic123",
		PushConfig: &PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
			Credentials: &credentials.Model{
				Username: "user",
				Password: pwd,
			},
		},
	}
	expected := &metrov1.Subscription{
		Name:  "projects/project123/subscriptions/subscription123",
		Topic: "projects/project123/topics/topic123",
		PushConfig: &metrov1.PushConfig{
			AuthenticationMethod: &metrov1.PushConfig_BasicAuth_{
				BasicAuth: &metrov1.PushConfig_BasicAuth{
					Username: "user",
					Password: pwd,
				},
			},
			PushEndpoint: "https://www.razorpay.com/api",
		},
	}
	resp := ModelToSubscriptionProtoV1(subscription)
	assert.Equal(t, expected, resp, "mismatch in model conversion")
}
