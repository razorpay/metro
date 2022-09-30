package scheduler

import (
	"testing"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/stretchr/testify/assert"
)

func TestRandomAlgoImpl_GetNode(t *testing.T) {
	ai, err := GetAlgorithmImpl(Random)
	assert.Nil(t, err)
	assert.NotNil(t, ai)

	nbs := []*nodebinding.Model{
		{
			ID:             uuid.New().String(),
			SubscriptionID: "sub1",
			NodeID:         "node01",
		},
		{
			ID:             uuid.New().String(),
			SubscriptionID: "sub2",
			NodeID:         "node03",
		},
	}

	nodeNames := []string{"node01", "node02", "node03", "node04", "node05", "node06"}
	nodes := make(map[string]*node.Model, len(nodeNames))
	for _, nodeName := range nodeNames {
		nodes[nodeName] = &node.Model{ID: nodeName}
	}

	schedulingNode, err := ai.GetNode(nbs, nodes)
	assert.Nil(t, err)
	assert.NotNil(t, schedulingNode)
	assert.Contains(t, nodeNames, schedulingNode.ID)
}
