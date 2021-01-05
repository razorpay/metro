// +build integration

package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Topic_CreateTopic(t *testing.T) {
	url := fmt.Sprintf("http://%s:8085", os.Getenv("METRO_TEST_HOST"))
	_, err := http.Get(url)
	assert.Nil(t, err)
}
