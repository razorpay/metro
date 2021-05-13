// +build unit

package customheap

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_DeadlineBasedPriorityQueue(t *testing.T) {

	ackMsgs := []*AckMessageWithDeadline{
		{MsgID: "msg-0", AckDeadline: 25},
		{MsgID: "msg-1", AckDeadline: 50},
		{MsgID: "msg-2", AckDeadline: 40},
		{MsgID: "msg-3", AckDeadline: 5},
	}

	pq := NewDeadlineBasedPriorityQueue()

	for i, item := range ackMsgs {
		pq.Indices = append(pq.Indices, item)
		pq.Indices[i].Index = i
		pq.MsgIDToIndexMapping[item.MsgID] = i
	}
	heap.Init(&pq)

	expected := []string{"msg-3", "msg-0", "msg-2", "msg-1"}

	var actual []string
	for len(pq.Indices) > 0 {
		pop := heap.Pop(&pq).(*AckMessageWithDeadline)
		actual = append(actual, pop.MsgID)
	}

	assert.Equal(t, actual, expected)
}
