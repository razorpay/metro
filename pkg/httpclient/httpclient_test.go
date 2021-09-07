// +build unit

package httpclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewClient(t *testing.T) {
	assert.Nil(t, NewClient(nil))

	assert.NotNil(t, NewClient(&Config{
		ConnectTimeoutMS:        10000,
		ConnKeepAliveMS:         0,
		ExpectContinueTimeoutMS: 0,
		IdleConnTimeoutMS:       60000,
		MaxAllIdleConns:         1000,
		MaxHostIdleConns:        1000,
		ResponseHeaderTimeoutMS: 25000,
		TLSHandshakeTimeoutMS:   2000,
	}))
}
