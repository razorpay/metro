// +build unit

package encryption

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_EncryptDecrypt_Success(t *testing.T) {
	RegisterEncryptionKey("2K9HQKejNV0OkycszeuZ7e6QKwtbwrzO")
	text := "Hello World"
	ciphertext, err1 := EncryptAsHexString([]byte(text))
	assert.Nil(t, err1)
	plaintext, err2 := DecryptFromHexString(ciphertext)
	assert.Nil(t, err2)
	assert.Equal(t, text, string(plaintext))
}

func Test_EncryptDecrypt_Failure(t *testing.T) {
	RegisterEncryptionKey("") // empty key
	text := "Hello World"
	ciphertext, err1 := EncryptAsHexString([]byte(text))
	assert.NotNil(t, err1)
	assert.Equal(t, ciphertext, "")
	_, err2 := DecryptFromHexString(ciphertext)
	assert.NotNil(t, err2)
}
