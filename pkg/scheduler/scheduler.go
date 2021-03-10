package scheduler

import (
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/subscription"
)

// Scheduler struct for subsciption scheduling on worker nodes
type Scheduler struct {
	algoImpl  IAlgoImpl
	Algorithm Algorithm
}

// New returns a new instance of scheduler
func New(algo Algorithm) (*Scheduler, error) {
	ai, err := GetAlgorithmImpl(algo)
	if err != nil {
		return nil, err
	}

	return &Scheduler{
		Algorithm: algo,
		algoImpl:  ai,
	}, nil

}

// Schedule schedules a subsciption on a node and returns a nodebinding model
func (s *Scheduler) Schedule(subscription *subscription.Model, nbs []*nodebinding.Model, nodes []*node.Model) (*nodebinding.Model, error) {
	node, err := s.algoImpl.GetNode(nbs, nodes)
	if err != nil {
		return nil, err
	}

	nb := nodebinding.Model{
		NodeID:         node.ID,
		SubscriptionID: subscription.Key(),
	}

	return &nb, nil
}
