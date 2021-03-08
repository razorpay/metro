package nodebinding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	nodebinding := getDummyNodeBindingModel()
	assert.Equal(t, nodebinding.Prefix(), "metro/nodebinding/")
}

func TestModel_Key(t *testing.T) {
	nodebinding := getDummyNodeBindingModel()
	assert.Equal(t, nodebinding.Key(), "metro/nodes/"+nodebinding.NodeID+"/"+nodebinding.SubscriptionID)
}

func getDummyNodeBindingModel() *Model {
	return &Model{
		NodeID:         "testID",
		SubscriptionID: "TestSubID",
	}
}
