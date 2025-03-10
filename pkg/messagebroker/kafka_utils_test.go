//go:build unit
// +build unit

package messagebroker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_normalizeTopicName(t *testing.T) {
	assert.Equal(t, "p1_s1", normalizeTopicName("p1/s1"))
	assert.Equal(t, "p1_s1_t1", normalizeTopicName("p1/s1/t1"))
}

func Test_getMessageID(t *testing.T) {
	m1 := getMessageID("m1")
	assert.Equal(t, "m1", m1)

	m2 := getMessageID("")
	assert.NotEmpty(t, m2)
}

func Test_convertRequestToKafkaHeaders(t *testing.T) {

	tNow := time.Now()
	attributes := make([]map[string][]byte, 0)
	attributes = append(attributes, map[string][]byte{
		"k1": []byte("v1"),
	})

	request := SendMessageToTopicRequest{
		Topic:      "t1",
		Message:    []byte("abc"),
		Attributes: attributes,
		MessageHeader: MessageHeader{
			MessageID:            "m1",
			PublishTime:          tNow,
			SourceTopic:          "st1",
			RetryTopic:           "rt1",
			Subscription:         "s1",
			CurrentRetryCount:    2,
			MaxRetryCount:        5,
			CurrentTopic:         "ct1",
			InitialDelayInterval: 10,
			CurrentDelayInterval: 50,
			ClosestDelayInterval: 120,
			DeadLetterTopic:      "dlt",
			NextDeliveryTime:     tNow,
		},
	}

	headers := convertRequestToKafkaHeaders(request)
	convertedRequest := convertKafkaHeadersToResponse(headers)

	assert.Equal(t, request.Attributes, convertedRequest.Attributes)
	assert.Equal(t, request.MessageHeader.MessageID, convertedRequest.MessageHeader.MessageID)
	assert.Equal(t, request.MessageHeader.PublishTime.Unix(), convertedRequest.MessageHeader.PublishTime.Unix())
	assert.Equal(t, request.MessageHeader.SourceTopic, convertedRequest.MessageHeader.SourceTopic)
	assert.Equal(t, request.MessageHeader.RetryTopic, convertedRequest.MessageHeader.RetryTopic)
	assert.Equal(t, request.MessageHeader.Subscription, convertedRequest.MessageHeader.Subscription)
	assert.Equal(t, request.MessageHeader.CurrentRetryCount, convertedRequest.MessageHeader.CurrentRetryCount)
	assert.Equal(t, request.MessageHeader.MaxRetryCount, convertedRequest.MessageHeader.MaxRetryCount)
	assert.Equal(t, request.MessageHeader.CurrentTopic, convertedRequest.MessageHeader.CurrentTopic)
	assert.Equal(t, request.MessageHeader.InitialDelayInterval, convertedRequest.MessageHeader.InitialDelayInterval)
	assert.Equal(t, request.MessageHeader.CurrentDelayInterval, convertedRequest.MessageHeader.CurrentDelayInterval)
	assert.Equal(t, request.MessageHeader.ClosestDelayInterval, convertedRequest.MessageHeader.ClosestDelayInterval)
	assert.Equal(t, request.MessageHeader.DeadLetterTopic, convertedRequest.MessageHeader.DeadLetterTopic)
	assert.Equal(t, request.MessageHeader.NextDeliveryTime.Unix(), convertedRequest.MessageHeader.NextDeliveryTime.Unix())
}

func Test_flattenMapSlice(t *testing.T) {
	attributes := make([]map[string][]byte, 1)
	attributes = append(attributes, map[string][]byte{
		"test-attribute": []byte("test-attribute-value"),
	})
	flattenedMap := flattenMapSlice(attributes)
	for _, attribute := range attributes {
		for key, value := range attribute {
			assert.NotNil(t, flattenedMap[key])
			assert.Equal(t, flattenedMap[key], string(value))
		}
	}
}
