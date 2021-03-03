package main

type AckMessageWithDeadline struct {
	MsgId       string
	AckDeadline int
	Index       int // The index of the item in the heap.
}

type DeadlineBasedPriorityQueue struct {
	indices             []*AckMessageWithDeadline
	msgIdToIndexMapping map[string]int
}

func NewDeadlineBasedPriorityQueue() DeadlineBasedPriorityQueue {
	return DeadlineBasedPriorityQueue{
		indices:             []*AckMessageWithDeadline{},
		msgIdToIndexMapping: make(map[string]int),
	}
}

func (pq DeadlineBasedPriorityQueue) Len() int { return len(pq.indices) }

func (pq DeadlineBasedPriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest based on expiration number as the priority
	// The lower the expiry, the higher the priority
	return pq.indices[i].AckDeadline < pq.indices[j].AckDeadline
}

// We just implement the pre-defined function in interface of heap.
func (pq *DeadlineBasedPriorityQueue) Pop() interface{} {
	old := pq.indices
	n := len(old)
	item := old[n-1]
	pq.indices = old[0 : n-1]
	return item
}

func (pq *DeadlineBasedPriorityQueue) Push(x interface{}) {
	n := len(pq.indices)
	item := x.(*AckMessageWithDeadline)
	item.Index = n
	pq.indices = append(pq.indices, item)

	pq.msgIdToIndexMapping[item.MsgId] = item.Index
}

func (pq DeadlineBasedPriorityQueue) Swap(i, j int) {
	pq.indices[i], pq.indices[j] = pq.indices[j], pq.indices[i]
	pq.indices[i].Index = i
	pq.indices[j].Index = j

	// track the current index of each msg_id as well
	pq.msgIdToIndexMapping[pq.indices[i].MsgId] = i
	pq.msgIdToIndexMapping[pq.indices[j].MsgId] = j
}
