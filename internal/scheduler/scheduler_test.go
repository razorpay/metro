// +build unit

package scheduler

import (
	"testing"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/stretchr/testify/assert"
)

func TestScheduler_Schedule(t *testing.T) {
	tests := []struct {
		algorithm Algorithm
		nodeID    []string
	}{
		{
			algorithm: Random,
			nodeID: []string{
				"node01",
				"node02",
			},
		},
		{
			algorithm: LoadBalance,
			nodeID: []string{
				"node02",
			},
		},
	}

	for _, test := range tests {
		sch, err := New(test.algorithm)
		assert.Nil(t, err)
		assert.NotNil(t, sch)

		sub := &subscription.Model{
			Name: "sub2",
		}
		sub.SetVersion("1")

		nbs := []*nodebinding.Model{
			{
				ID:             uuid.New().String(),
				SubscriptionID: "sub1",
				NodeID:         "node01",
			},
		}

		nodes := []*node.Model{
			{
				ID: "node01",
			},
			{
				ID: "node02",
			},
		}

		nb, err := sch.Schedule(sub, nbs, nodes, 0)
		assert.Nil(t, err)
		assert.NotNil(t, nb)
		assert.Equal(t, nb.SubscriptionID, "sub2")
		assert.Contains(t, test.nodeID, nb.NodeID)
	}
}
