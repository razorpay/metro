// +build integration

package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_HealthCheck(t *testing.T) {
	url := fmt.Sprintf("http://%s:8082/v1/healthcheck", os.Getenv("METRO_TEST_HOST"))
	_, err := http.Get(url)
	assert.Nil(t, err)
}
