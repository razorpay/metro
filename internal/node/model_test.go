package node

import (
	"testing"

	"github.com/razorpay/metro/internal/common"

	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	node := getDummyNodeModel()
	assert.Equal(t, node.Prefix(), common.GetBasePrefix()+"nodes/")
}

func TestModel_Key(t *testing.T) {
	node := getDummyNodeModel()
	assert.Equal(t, node.Key(), common.GetBasePrefix()+node.ID)
}

func getDummyNodeModel() *Model {
	return &Model{
		ID: "testID",
	}
}
