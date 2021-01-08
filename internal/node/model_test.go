package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	node := getDummyNodeModel()
	assert.Equal(t, node.Prefix(), "registry/nodes/")
}

func TestModel_Key(t *testing.T) {
	node := getDummyNodeModel()
	assert.Equal(t, node.Key(), "registry/projects/"+node.ID)
}

func getDummyNodeModel() *Model {
	return &Model{
		ID:   "test",
		Name: "test",
	}
}
