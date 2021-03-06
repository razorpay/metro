package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	node := getDummyNodeModel()
	assert.Equal(t, node.Prefix(), "metro/nodes/")
}

func TestModel_Key(t *testing.T) {
	node := getDummyNodeModel()
	assert.Equal(t, node.Key(), "metro/nodes/"+node.ID)
}

func getDummyNodeModel() *Model {
	return &Model{
		ID: "testID",
	}
}
