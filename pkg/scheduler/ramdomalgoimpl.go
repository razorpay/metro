package scheduler

import (
	"fmt"
	rand2 "math/rand"

	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
)

// RandomAlgoImpl implements a random scheduling algorithm
type RandomAlgoImpl struct {
}

// GetNode method returns the selected node for scheduling selecting one randomly
func (algo *RandomAlgoImpl) GetNode(nodebindings []nodebinding.Model, nodes []node.Model) (*node.Model, error) {
	if len(nodes) <= 0 {
		return nil, fmt.Errorf("no node available for scheduling")
	}

	rand := rand2.Intn(len(nodes))
	return &nodes[rand], nil
}
