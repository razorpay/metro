//go:build unit
// +build unit

package customheap

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_OffsetBasedPriorityQueue(t *testing.T) {

	ackMsgs := []*AckMessageWithOffset{
		{MsgID: "msg-0", Offset: 3},
		{MsgID: "msg-1", Offset: 1},
		{MsgID: "msg-2", Offset: 0},
		{MsgID: "msg-3", Offset: 2},
	}

	pq := NewOffsetBasedPriorityQueue()

	for i, item := range ackMsgs {
		pq.Indices = append(pq.Indices, item)
		pq.Indices[i].Index = i
		pq.MsgIDToIndexMapping[item.MsgID] = i
	}
	heap.Init(&pq)

	expected := []string{"msg-2", "msg-1", "msg-3", "msg-0"}

	var actual []string
	for len(pq.Indices) > 0 {
		pop := heap.Pop(&pq).(*AckMessageWithOffset)
		actual = append(actual, pop.MsgID)
	}

	assert.Equal(t, actual, expected)
}
