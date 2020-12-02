package messagebroker

import "time"

type MessageConsumer interface {

	//GetMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
	//from the previous committed offset. If the available messages in the queue are less, returns
	// how many ever messages are availble
	GetMessages(numOfMessages int, timeout time.Duration) ([]string, error)

	//Commits messages if any
	//This func will commit the message consumed
	//by all the previous calls to GetMessages
	Commit()
}
