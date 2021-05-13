// +build unit

package nodebinding

import (
	"testing"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	nodebinding := getDummyNodeBindingModel()
	assert.Equal(t, nodebinding.Prefix(), common.GetBasePrefix()+"nodebinding/")
}

func TestModel_Key(t *testing.T) {
	nodebinding := getDummyNodeBindingModel()
	assert.Equal(t, nodebinding.Key(), nodebinding.Prefix()+nodebinding.NodeID+"/"+nodebinding.SubscriptionID+"_"+nodebinding.ID[0:4])
}

func getDummyNodeBindingModel() *Model {
	return &Model{
		ID:             uuid.New().String(),
		NodeID:         "testID",
		SubscriptionID: "TestSubID",
	}
}
