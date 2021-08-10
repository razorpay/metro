// +build unit

package stream

import (
	"github.com/golang/protobuf/jsonpb"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

func Test_ProtoMarhshalUnMarshal(t *testing.T) {

	const layout = "Jan 2, 2006 at 3:04pm (MST)"
	tm, _ := time.Parse(layout, "Jan 1, 2021 at 12:05pm (IST)") // using fixed time for deterministic test results

	originalReq := &metrov1.PushEndpointRequest{
		Message: &metrov1.PubsubMessage{
			Data: []byte("abc"),
			Attributes: map[string]string{
				"k1": "v1",
				"k2": "v2",
			},
			MessageId:   "msg-1",
			PublishTime: timestamppb.New(tm),
			OrderingKey: "ok-1",
		},
		Subscription: "subs-1",
	}

	// marshal
	reqBytes := getRequestBytes(originalReq)
	reqAsString := reqBytes.String()
	// match the string generated after proto marshalling
	assert.Equal(t, reqAsString, "{\"message\":{\"data\":\"YWJj\",\"attributes\":{\"k1\":\"v1\",\"k2\":\"v2\"},\"messageId\":\"msg-1\",\"publishTime\":\"2021-01-01T06:35:00Z\",\"orderingKey\":\"ok-1\"},\"subscription\":\"subs-1\"}")

	// unmarshal
	currentReq := metrov1.PushEndpointRequest{}
	jsonpb.Unmarshal(reqBytes, &currentReq)

	assert.Equal(t, originalReq.Subscription, currentReq.Subscription)
	assert.Equal(t, originalReq.Message.MessageId, currentReq.Message.MessageId)
	assert.Equal(t, originalReq.Message.OrderingKey, currentReq.Message.OrderingKey)
	assert.Equal(t, originalReq.Message.Data, currentReq.Message.Data)
	assert.Equal(t, originalReq.Message.Attributes, currentReq.Message.Attributes)
	assert.Equal(t, originalReq.Message.PublishTime, currentReq.Message.PublishTime)
}
