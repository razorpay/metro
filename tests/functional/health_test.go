// +build functional

package functional

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_HealthCheck(t *testing.T) {
	url := fmt.Sprintf("%s/v1/healthcheck", metroHttpHost)
	_, err := http.Get(url)
	assert.Nil(t, err)
}
