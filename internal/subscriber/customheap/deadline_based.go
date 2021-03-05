package customheap

import "time"

// AckMessageWithDeadline ...
type AckMessageWithDeadline struct {
	MsgID       string
	AckDeadline int32
	Index       int // The index of the item in the customheap.
}

// HasHitDeadline ...
func (ackMsg *AckMessageWithDeadline) HasHitDeadline() bool {
	return int64(ackMsg.AckDeadline) > time.Now().Unix()
}

// DeadlineBasedPriorityQueue ...
type DeadlineBasedPriorityQueue struct {
	Indices             []*AckMessageWithDeadline
	MsgIDToIndexMapping map[string]int
}

// NewDeadlineBasedPriorityQueue ...
func NewDeadlineBasedPriorityQueue() DeadlineBasedPriorityQueue {
	return DeadlineBasedPriorityQueue{
		Indices:             []*AckMessageWithDeadline{},
		MsgIDToIndexMapping: make(map[string]int),
	}
}

// Len ...
func (pq DeadlineBasedPriorityQueue) Len() int { return len(pq.Indices) }

// Less ...
func (pq DeadlineBasedPriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest based on expiration number as the priority
	// The lower the expiry, the higher the priority
	return pq.Indices[i].AckDeadline < pq.Indices[j].AckDeadline
}

// Pop we just implement the pre-defined function in interface of heap.
func (pq *DeadlineBasedPriorityQueue) Pop() interface{} {
	old := pq.Indices
	n := len(old)
	item := old[n-1]
	pq.Indices = old[0 : n-1]
	return item
}

// Push ...
func (pq *DeadlineBasedPriorityQueue) Push(x interface{}) {
	n := len(pq.Indices)
	item := x.(*AckMessageWithDeadline)
	item.Index = n
	pq.Indices = append(pq.Indices, item)

	pq.MsgIDToIndexMapping[item.MsgID] = item.Index
}

// Swap ...
func (pq DeadlineBasedPriorityQueue) Swap(i, j int) {
	pq.Indices[i], pq.Indices[j] = pq.Indices[j], pq.Indices[i]
	pq.Indices[i].Index = i
	pq.Indices[j].Index = j

	// track the current index of each msg_id as well
	pq.MsgIDToIndexMapping[pq.Indices[i].MsgID] = i
	pq.MsgIDToIndexMapping[pq.Indices[j].MsgID] = j
}
