// +build unit

package messagebroker

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_NewPartitionOffset(t *testing.T) {
	po := NewPartitionOffset(10, 35)
	assert.Equal(t, po.String(), "[10]-[35]")
}

func Test_NewReceivedMessage(t *testing.T) {
	data, _ := json.Marshal("abc")
	tNow := time.Now()
	rm := ReceivedMessage{
		Data: data,
		MessageHeader: MessageHeader{
			MessageID:            "m1",
			PublishTime:          tNow,
			SourceTopic:          "t1",
			Subscription:         "s1",
			CurrentRetryCount:    2,
			MaxRetryCount:        5,
			CurrentTopic:         "ct1",
			InitialDelayInterval: 30,
			DeadLetterTopic:      "dl",
			NextDeliveryTime:     tNow,
		},
	}

	assert.Equal(t, rm.MessageHeader.String(), fmt.Sprintf("message_id=[m1],publish_time=[%v],source_topic=[t1],subscription=[s1],current_retry_count=[2],max_retry_count=[5],current_topic=[ct1],initial_delay_interval=[30],dead_letter_topic=[dl],next_delivery_time=[%v]", tNow.Unix(), tNow.Unix()))
}

func Test_pulsarAckMessage(t *testing.T) {
	po := pulsarAckMessage{ID: "abc"}
	data, _ := json.Marshal("abc")
	assert.Equal(t, po.Serialize(), data)
}
