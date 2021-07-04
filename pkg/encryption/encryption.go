package encryption

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/gtank/cryptopasta"
	"github.com/pkg/errors"
)

var (
	mainEncryptionKey        string
	errEncryptionKeyNotFound = errors.New("encryption key is not initialized")
)

// RegisterEncryptionKey initializes an encryption key to be used during all encrypt/decrypt operations
func RegisterEncryptionKey(key string) {
	mainEncryptionKey = key
}

// EncryptAsHexString encrypts given bytes and returns equivalent hexadecimal string
func EncryptAsHexString(data []byte) (string, error) {
	if mainEncryptionKey == "" {
		return "", errEncryptionKeyNotFound
	}

	encryptedBytes, err := encryptWithKey(data, mainEncryptionKey)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", encryptedBytes), nil
}

func encryptWithKey(data []byte, encryptionKey string) ([]byte, error) {
	var key [32]byte
	copy(key[:], string(encryptionKey[:]))

	encryptedText, err := cryptopasta.Encrypt(data, &key)
	if err != nil {
		return nil, err
	}
	return encryptedText, nil
}

// DecryptFromHexString decrypts given hexadecimal string and returns equivalent bytes
func DecryptFromHexString(hexString string) ([]byte, error) {
	if mainEncryptionKey == "" {
		return nil, errEncryptionKeyNotFound
	}

	h, err := hex.DecodeString(hexString)
	if err != nil {
		return nil, err
	}

	return decryptWithKey(bytes.NewBuffer(h).Bytes(), mainEncryptionKey)
}

func decryptWithKey(data []byte, encryptionKey string) ([]byte, error) {
	var key [32]byte
	copy(key[:], string(encryptionKey[:]))

	decryptedText, err := cryptopasta.Decrypt(data, &key)
	if err != nil {
		return nil, err
	}
	return decryptedText, nil
}
