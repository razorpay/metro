// +build unit

package credentials

import (
	"testing"

	"github.com/razorpay/metro/internal/common"
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
	return &Model{
		Username:  "project123__c525c7",
		Password:  "l0laNoI360l4uvD96682",
		ProjectID: "project123",
	}
}
