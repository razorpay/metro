// +build unit

package httpclient

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"testing"

	"github.com/razorpay/metro/tests/pushserver"
	"github.com/stretchr/testify/assert"
)

var ps *pushserver.PushServer
var chanMap = map[string]chan pushserver.PushMessage{}

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

func Test_SendRequest(t *testing.T) {
	ps = pushserver.StartServer(context.TODO(), chanMap)
	defer ps.StopServer()

	connectionsCreated := 0
	clientTrace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if !info.Reused {
				connectionsCreated += 1
			}
		},
	}

	traceCtx := httptrace.WithClientTrace(context.Background(), clientTrace)
	client := NewClient(&Config{
		ConnectTimeoutMS:        10000,
		ConnKeepAliveMS:         100,
		ExpectContinueTimeoutMS: 100,
		IdleConnTimeoutMS:       60000,
		MaxAllIdleConns:         1000,
		MaxHostIdleConns:        1000,
		ResponseHeaderTimeoutMS: 25000,
		TLSHandshakeTimeoutMS:   2000,
	})

	for i := 0; i < 10; i++ {
		req, _ := http.NewRequestWithContext(traceCtx, http.MethodGet, "http://localhost:8077/push", nil)
		SendRequest(client, req)
	}

	// One connection should be created
	assert.Equal(t, 1, connectionsCreated)
}
