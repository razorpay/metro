// +build unit

package credentials

import (
	"testing"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/pkg/encryption"
	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	credentials := getDummyCredentials()
	assert.Equal(t, credentials.Prefix(), common.GetBasePrefix()+"credentials/"+credentials.ProjectID+"/")
}

func TestModel_Key(t *testing.T) {
	credentials := getDummyCredentials()
	assert.Equal(t, credentials.Key(), credentials.Prefix()+credentials.Username)
}

func getDummyCredentials() *Model {
	encryption.RegisterEncryptionKey("key")
	pwd, _ := encryption.EncryptAsHexString([]byte("password"))
	return &Model{
		Username:  "project123__c525c7",
		Password:  pwd,
		ProjectID: "project123",
	}
}

func TestModel_HiddenPassword(t *testing.T) {
	credentials := getDummyCredentials()
	expectedHiddenPassword := AsteriskString + "word"
	hiddenPwd, err := credentials.GetHiddenPassword()
	assert.Nil(t, err)
	assert.Equal(t, expectedHiddenPassword, hiddenPwd)
}

func TestModel_HiddenPassword_Failure(t *testing.T) {
	credentials := getDummyCredentials()
	credentials.Password = "SHo5MThKVTQ2NjVYMGg5dllvRjk="
	hiddenPwd, err := credentials.GetHiddenPassword()
	assert.Empty(t, hiddenPwd)
	assert.Equal(t, err, ErrPasswordNotInExpectedFormat)
}

func TestNewCredential(t *testing.T) {
	credential := NewCredential("username", "password")
	assert.NotNil(t, credential)
	assert.Equal(t, "username", credential.GetUsername())
	assert.NotNil(t, credential.GetProjectID())
}
