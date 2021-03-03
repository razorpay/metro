package main

import (
	"container/heap"
	"fmt"
	"time"
)

func main() {

	ackMsgs1 := []*AckMessageWithOffset{
		{MsgId: "msg-0", Offset: 0},
		{MsgId: "msg-1", Offset: 4},
		{MsgId: "msg-2", Offset: 3},
		{MsgId: "msg-3", Offset: 1},
		{MsgId: "msg-4", Offset: 5},
		{MsgId: "msg-5", Offset: 2},
		{MsgId: "msg-6", Offset: 7},
	}

	ackMsgs2 := []*AckMessageWithDeadline{
		{MsgId: "msg-0", AckDeadline: 25},
		{MsgId: "msg-1", AckDeadline: 50},
		{MsgId: "msg-2", AckDeadline: 20},
		{MsgId: "msg-3", AckDeadline: 30},
		{MsgId: "msg-4", AckDeadline: 10},
		{MsgId: "msg-5", AckDeadline: 40},
		{MsgId: "msg-6", AckDeadline: 5},
	}

	pq1 := NewOffsetBasedPriorityQueue()
	pq2 := NewDeadlineBasedPriorityQueue()

	for i, item := range ackMsgs1 {
		pq1.indices = append(pq1.indices, item)
		pq1.indices[i].Index = i
		pq1.msgIdToIndexMapping[item.MsgId] = i
	}

	for j, item := range ackMsgs2 {
		pq2.indices = append(pq2.indices, item)
		pq2.indices[j].Index = j
		pq2.msgIdToIndexMapping[item.MsgId] = j
	}

	heap.Init(&pq1)
	heap.Init(&pq2)

	// TODO : group messages based on timestamp in a list
	// TODO : eviction ticket and push ticker put in the subscriber event loop itself
	// 10 20 30 40 50
	// 10:{1,2,3} 20:{4,5} 30:{6,7,8,9,10}

	finished := make(chan bool)

	// auto-eviction goroutine
	go func(finished chan bool) {

		for {

			if len(pq1.indices) == 0 {
				fmt.Println("no items left on heap...")
				break
			}

			// since the min most element eligible for eviction will always be at the head of the slice
			peek := pq2.indices[0]
			if peek.AckDeadline > 1 {
				fmt.Println("Before MsgIdToIndexMapping pq1: ", pq1.msgIdToIndexMapping)
				fmt.Println("Before MsgIdToIndexMapping pq2: ", pq2.msgIdToIndexMapping)

				item2 := heap.Pop(&pq2).(*AckMessageWithDeadline)
				fmt.Println(fmt.Sprintf("auto==> msgId:[%v], deadline:[%v], index:[%v]", item2.MsgId, item2.AckDeadline, item2.Index))

				item1 := heap.Remove(&pq1, pq1.msgIdToIndexMapping[item2.MsgId]).(*AckMessageWithOffset)
				fmt.Println(fmt.Sprintf("auto==> msgId:[%v], offset:[%v], index:[%v]", item1.MsgId, item1.Offset, item1.Index))

				delete(pq1.msgIdToIndexMapping, item2.MsgId)

				fmt.Println("After MsgIdToIndexMapping pq1: ", pq1.msgIdToIndexMapping)
				fmt.Println("After MsgIdToIndexMapping pq2: ", pq2.msgIdToIndexMapping)

				var str1, str2 string
				for i, ackMsg1 := range pq1.indices {
					str1 += fmt.Sprintf("%v:%v,", ackMsg1.MsgId, i)
				}

				for j, ackMsg2 := range pq2.indices {
					str2 += fmt.Sprintf("%v:%v,", ackMsg2.MsgId, j)
				}
				fmt.Println("actual array indexes for pq1 : ", str1)
				fmt.Println("actual array indexes for pq2 : ", str2)

			}

			fmt.Println("===========================")
			time.Sleep(time.Second * 2)
		}

		finished <- true
	}(finished)

	<-finished
}
