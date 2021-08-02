// +build unit

package messagebroker

import (
	"encoding/json"
	"testing"

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
