package scheduler

import (
	"fmt"
	"sort"

	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
)

// NodeCount holds counts of the scheduled subscription on a node
type NodeCount struct {
	Key   string
	Count int
}

// NodeCountList represents array of NodeCount
type NodeCountList []NodeCount

func (p NodeCountList) Len() int           { return len(p) }
func (p NodeCountList) Less(i, j int) bool { return p[i].Count < p[j].Count }
func (p NodeCountList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// LoadBalanceAlgoImpl implements load based scheduler algorithm
type LoadBalanceAlgoImpl struct {
}

// GetNode method returns the selected node for scheduling based on current load
func (algo *LoadBalanceAlgoImpl) GetNode(nodebindings []*nodebinding.Model, nodes []*node.Model) (*node.Model, error) {
	if len(nodes) <= 0 {
		return nil, fmt.Errorf("no node available for scheduling")
	}

	nodeCounts := map[string]int{}
	nodeCountList := NodeCountList{}

	for _, node := range nodes {
		nodeCounts[node.ID] = 0
	}

	for _, nodebinding := range nodebindings {
		nodeCounts[nodebinding.NodeID]++
	}

	for k, v := range nodeCounts {
		nodeCountList = append(nodeCountList, NodeCount{
			Key:   k,
			Count: v,
		})
	}

	sort.Sort(nodeCountList)

	for _, node := range nodes {
		if node.ID == nodeCountList[0].Key {
			return node, nil
		}
	}

	return nil, fmt.Errorf("node not found")
}
