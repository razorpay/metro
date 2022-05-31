package scheduler

import (
	"fmt"

	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
)

// Algorithm name of the algo for scheduling
type Algorithm string

const (
	// Random Algo type
	Random Algorithm = "Random"

	// LoadBalance algo type
	LoadBalance Algorithm = "LoadBalance"
)

var (
	// ErrInvalidAlgorithm error for invalid algorithm input
	ErrInvalidAlgorithm = fmt.Errorf("invalid Algorithm")
)

var algorithmMap = map[Algorithm]IAlgoImpl{
	LoadBalance: new(LoadBalanceAlgoImpl),
	Random:      new(RandomAlgoImpl),
}

// IAlgoImpl defines interface for scheduling algorithm implementations
type IAlgoImpl interface {
	// GetNode method returns the selected node for scheduling
	GetNode(nodebindings []*nodebinding.Model, nodes map[string]*node.Model) (*node.Model, error)
}

// GetAlgorithmImpl is a factory method which returns the required impl of the algorithm
func GetAlgorithmImpl(algorithm Algorithm) (IAlgoImpl, error) {
	ai := algorithmMap[algorithm]
	if ai == nil {
		return nil, ErrInvalidAlgorithm
	}
	return ai, nil
}
