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
		Data:        data,
		MessageID:   "m1",
		Topic:       "t1",
		Partition:   11,
		Offset:      99,
		RetryCount:  0,
		PublishTime: tNow,
	}

	assert.Equal(t, rm.String(), fmt.Sprintf("data=[%v], msgId=[%v], topic=[%v], partition=[%v], offset=[%v], publishTime=[%v]",
		string(rm.Data), rm.MessageID, rm.Topic, rm.Partition, rm.Offset, tNow.Unix()))
}

func Test_pulsarAckMessage(t *testing.T) {
	po := pulsarAckMessage{ID: "abc"}
	data, _ := json.Marshal("abc")
	assert.Equal(t, po.Serialize(), data)
}
