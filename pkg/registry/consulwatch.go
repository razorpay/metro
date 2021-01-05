package registry

import (
	"fmt"

	"github.com/hashicorp/consul/api"
)

// ConsulWatchHandler implements consul watch handler and stores the users handler function
type ConsulWatchHandler struct {
	handlerFunc HandlerFunc
}

// NewConsulWatchHandler is used to create a new struct of type WatchHandler
func NewConsulWatchHandler(hfunc HandlerFunc) *ConsulWatchHandler {
	return &ConsulWatchHandler{
		handlerFunc: hfunc,
	}
}

// Handler implements the consul watch handler method and invokes the requester handler
func (cw *ConsulWatchHandler) Handler(index uint64, result interface{}) {
	pairs, ok := result.(api.KVPairs)
	if !ok {
		// Todo: decide what to do in case consul schema is corrupted
		return
	}

	fmt.Println(pairs)
	results := []Pair{}

	for i := range pairs {
		results = append(results, Pair{
			key:   pairs[i].Key,
			value: pairs[i].Value,
		})
	}

	cw.handlerFunc(results)
}
