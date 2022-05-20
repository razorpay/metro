//go:build unit
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

// func TestNewCredential(t *testing.T) {
// 	type args struct {
// 		username string
// 		password string
// 	}
// 	password := "password"
// 	pwd, _ := encryption.EncryptAsHexString([]byte(password))
// 	credentials := &Model{
// 		Username:  "project123__c525c7",
// 		Password:  pwd,
// 		ProjectID: "project123",
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 		want *Model
// 	}{
// 		{
// 			name: "Test 1",
// 			args: args{
// 				username: credentials.Username,
// 				password: password,
// 			},
// 			want: credentials,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			println("-" + tt.args.password + "-")
// 			println([]byte(tt.args.password))
// 			println(encryption.EncryptAsHexString([]byte(password)))
// 			if got := NewCredential(tt.args.username, tt.args.password); !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("NewCredential() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
