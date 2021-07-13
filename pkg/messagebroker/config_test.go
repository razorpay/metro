// +build unit

package messagebroker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getCertFile_Success(t *testing.T) {
	path, err := getCertFile("testdata/", "user-cert.pem")
	assert.NotEmpty(t, path)
	assert.Nil(t, err)
}

func Test_getCertFile_Failure(t *testing.T) {
	path, err := getCertFile("random-dir/", "user-cert.pem")
	assert.Empty(t, path)
	assert.NotNil(t, err)
}
