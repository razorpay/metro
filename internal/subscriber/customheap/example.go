package customheap

import (
	"container/heap"
	"fmt"
	"time"
)

func main() {

	// TODO: convert to UT

	ackMsgs1 := []*AckMessageWithOffset{
		{MsgID: "msg-0", Offset: 0},
		{MsgID: "msg-1", Offset: 4},
		{MsgID: "msg-2", Offset: 3},
		{MsgID: "msg-3", Offset: 1},
		{MsgID: "msg-4", Offset: 5},
		{MsgID: "msg-5", Offset: 2},
		{MsgID: "msg-6", Offset: 7},
	}

	ackMsgs2 := []*AckMessageWithDeadline{
		{MsgID: "msg-0", AckDeadline: 25},
		{MsgID: "msg-1", AckDeadline: 50},
		{MsgID: "msg-2", AckDeadline: 20},
		{MsgID: "msg-3", AckDeadline: 30},
		{MsgID: "msg-4", AckDeadline: 10},
		{MsgID: "msg-5", AckDeadline: 40},
		{MsgID: "msg-6", AckDeadline: 5},
	}

	pq1 := NewOffsetBasedPriorityQueue()
	pq2 := NewDeadlineBasedPriorityQueue()

	for _, item := range ackMsgs1 {
		pq1.Indices = append(pq1.Indices, item)
		//pq1.Indices[i].Index = i
		//pq1.MsgIDToIndexMapping[item.MsgID] = i
	}

	for _, item := range ackMsgs2 {
		pq2.Indices = append(pq2.Indices, item)
		//pq2.Indices[j].Index = j
		//pq2.MsgIDToIndexMapping[item.MsgID] = j
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

			if len(pq1.Indices) == 0 {
				fmt.Println("no items left on customheap...")
				break
			}

			// since the min most element eligible for eviction will always be at the head of the slice
			peek := pq2.Indices[0]
			if peek.AckDeadline > 1 {
				fmt.Println("Before MsgIDToIndexMapping pq1: ", pq1.MsgIDToIndexMapping)
				fmt.Println("Before MsgIDToIndexMapping pq2: ", pq2.MsgIDToIndexMapping)

				item2 := heap.Pop(&pq2).(*AckMessageWithDeadline)
				fmt.Println(fmt.Sprintf("auto==> msgId:[%v], deadline:[%v], index:[%v]", item2.MsgID, item2.AckDeadline, item2.Index))

				item1 := heap.Remove(&pq1, pq1.MsgIDToIndexMapping[item2.MsgID]).(*AckMessageWithOffset)
				fmt.Println(fmt.Sprintf("auto==> msgId:[%v], offset:[%v], index:[%v]", item1.MsgID, item1.Offset, item1.Index))

				delete(pq1.MsgIDToIndexMapping, item2.MsgID)

				fmt.Println("After MsgIDToIndexMapping pq1: ", pq1.MsgIDToIndexMapping)
				fmt.Println("After MsgIDToIndexMapping pq2: ", pq2.MsgIDToIndexMapping)

				var str1, str2 string
				for i, ackMsg1 := range pq1.Indices {
					str1 += fmt.Sprintf("%v:%v,", ackMsg1.MsgID, i)
				}

				for j, ackMsg2 := range pq2.Indices {
					str2 += fmt.Sprintf("%v:%v,", ackMsg2.MsgID, j)
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
