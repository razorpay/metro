// +build unit

package messagebroker

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_NewPartitionOffset(t *testing.T) {
	po := NewPartitionOffset(10, 35)
	assert.Equal(t, po.String(), "[10]-[35]")
}

func Test_pulsarAckMessage(t *testing.T) {
	po := pulsarAckMessage{ID: "abc"}
	data, _ := json.Marshal("abc")
	assert.Equal(t, po.Serialize(), data)
}

func Test_GetMessagesFromTopicResponse(t *testing.T) {
	msgs := make([]ReceivedMessage, 0)
	msgs = append(msgs, ReceivedMessage{})
	resp := GetMessagesFromTopicResponse{Messages: msgs}
	assert.True(t, resp.HasNonZeroMessages())
}

func Test_ReceivedMessage(t *testing.T) {
	tNow := time.Now()
	tPast := tNow.Add(time.Second * 100 * -1)

	rm := ReceivedMessage{
		Data:       bytes.NewBufferString("abc").Bytes(),
		Topic:      "t1",
		Partition:  10,
		Offset:     234,
		Attributes: nil,
		MessageHeader: MessageHeader{
			MessageID:            "m1",
			PublishTime:          tNow,
			SourceTopic:          "st1",
			RetryTopic:           "rt1",
			Subscription:         "s1",
			CurrentRetryCount:    2,
			MaxRetryCount:        10,
			CurrentTopic:         "ct1",
			InitialDelayInterval: 10,
			CurrentDelayInterval: 90,
			ClosestDelayInterval: 150,
			DeadLetterTopic:      "dlt1",
			NextDeliveryTime:     tPast,
		},
	}

	assert.True(t, rm.CanProcessMessage())
	assert.False(t, rm.HasReachedRetryThreshold())
	assert.NotNil(t, rm.MessageHeader.LogFields())
}
