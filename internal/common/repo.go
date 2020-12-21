package common

import (
	"encoding/json"

	"github.com/razorpay/metro/pkg/registry"
)

const (
	basePrefix = "registry/"
)

// Repo implements base repo methods
type Repo struct {
	Registry registry.Registry
}

// Create puts the model in the registry
func (r Repo) Create(m Model) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return r.Registry.Put(basePrefix+m.Prefix()+m.Key(), b)
}
