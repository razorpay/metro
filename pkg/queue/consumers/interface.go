package consumer

import "time"

type QueueConsumer interface {

	//GetMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
	//from the previous committed offset. If the available messages in the queue are less, returns
	// how many ever messages are availble
	GetMessages(numOfMessages int, timeout time.Duration) ([]string, error)

	//GetUnCommitedMessages returns the messages read in the previous call
	//to GetMessages - if they are not yet committed.
	// This is especially usefull in the cases where a client
	//consuming the messsages crashes and when a new instance comes back
	//it first needs to call this.
	GetUnCommitedMessages(numOfMessages int) []string

	//Commits messages if any
	//This func will commit the message consumed
	//by the previous call to GetMessages
	Commit()
}
