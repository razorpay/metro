package nodebinding

import (
	"testing"

	"github.com/razorpay/metro/internal/common"

	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	nodebinding := getDummyNodeBindingModel()
	assert.Equal(t, nodebinding.Prefix(), common.GetBasePrefix()+"nodebinding/")
}

func TestModel_Key(t *testing.T) {
	nodebinding := getDummyNodeBindingModel()
	assert.Equal(t, nodebinding.Key(), nodebinding.Prefix()+nodebinding.NodeID+"/"+nodebinding.SubscriptionID)
}

func getDummyNodeBindingModel() *Model {
	return &Model{
		NodeID:         "testID",
		SubscriptionID: "TestSubID",
	}
}
