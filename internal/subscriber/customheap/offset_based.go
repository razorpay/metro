package customheap

// AckMessageWithOffset ...
type AckMessageWithOffset struct {
	MsgID  string
	Offset int32
	Index  int // The index of the item in the customheap.
}

// OffsetBasedPriorityQueue ...
type OffsetBasedPriorityQueue struct {
	Indices             []*AckMessageWithOffset
	MsgIDToIndexMapping map[string]int
}

// NewOffsetBasedPriorityQueue ...
func NewOffsetBasedPriorityQueue() OffsetBasedPriorityQueue {
	return OffsetBasedPriorityQueue{
		Indices:             make([]*AckMessageWithOffset, 0),
		MsgIDToIndexMapping: make(map[string]int),
	}
}

// Len ...
func (pq OffsetBasedPriorityQueue) Len() int { return len(pq.Indices) }

// Less ...
func (pq OffsetBasedPriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest based on expiration number as the priority
	// The lower the expiry, the higher the priority
	return pq.Indices[i].Offset < pq.Indices[j].Offset
}

// Pop we just implement the pre-defined function in interface of heap
func (pq *OffsetBasedPriorityQueue) Pop() interface{} {
	old := pq.Indices
	n := len(old)
	item := old[n-1]
	pq.Indices = old[0 : n-1]
	return item
}

// Push ...
func (pq *OffsetBasedPriorityQueue) Push(x interface{}) {
	n := len(pq.Indices)
	item := x.(*AckMessageWithOffset)
	item.Index = n
	pq.Indices = append(pq.Indices, item)

	pq.MsgIDToIndexMapping[item.MsgID] = item.Index
}

// Swap ...
func (pq OffsetBasedPriorityQueue) Swap(i, j int) {
	pq.Indices[i], pq.Indices[j] = pq.Indices[j], pq.Indices[i]
	pq.Indices[i].Index = i
	pq.Indices[j].Index = j

	// track the current index of each msg_id as well
	pq.MsgIDToIndexMapping[pq.Indices[i].MsgID] = i
	pq.MsgIDToIndexMapping[pq.Indices[j].MsgID] = j
}
