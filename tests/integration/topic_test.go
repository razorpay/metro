// +build integration

package integration

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Topic_CreateTopic(t *testing.T) {
	_, err := http.Get("http://metro-producer:8082/v1/healthcheck")
	assert.Nil(t, err)
	t.Logf("error : %s", err.Error())
	//assert.Equal(t, 200, resp.StatusCode)
}
