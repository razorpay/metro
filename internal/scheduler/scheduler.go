package scheduler

import (
	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/subscription"
)

// IScheduler is the interface implemented by Schedulers
type IScheduler interface {
	// Schedule implemented by Scheduler based on current subscription and current load on nodes
	Schedule(*subscription.Model, int, []*nodebinding.Model, map[string]*node.Model) (*nodebinding.Model, error)
}

// Scheduler struct for subsciption scheduling on worker nodes
type Scheduler struct {
	algoImpl  IAlgoImpl
	Algorithm Algorithm
}

// New returns a new instance of scheduler
func New(algo Algorithm) (IScheduler, error) {
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
func (s *Scheduler) Schedule(sub *subscription.Model, partition int, nbs []*nodebinding.Model, nodes map[string]*node.Model) (*nodebinding.Model, error) {
	node, err := s.algoImpl.GetNode(nbs, nodes)
	if err != nil {
		return nil, err
	}
	subVersion := sub.GetVersion()

	nb := nodebinding.Model{
		ID:                  uuid.New().String(),
		NodeID:              node.ID,
		SubscriptionID:      sub.Name,
		SubscriptionVersion: subVersion,
		Partition:           partition,
	}

	return &nb, nil
}
