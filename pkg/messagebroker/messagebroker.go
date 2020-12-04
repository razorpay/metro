package messagebroker

import "time"

// all brokers need to comply to this
type Broker interface {
	Admin
	Producer
	Consumer
}

// for admin operations on topics, partitions, updating schema registry etc
type Admin interface {
	CreateTopic(topic string) error

	DeleteTopic(topic string) error
}

// for produce operations
type Producer interface {
	Produce(topic string, message []byte) (string, error)
}

type Consumer interface {
	//GetMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
	//from the previous committed offset. If the available messages in the queue are less, returns
	// how many ever messages are availble
	GetMessages(numOfMessages int, timeout time.Duration) ([]string, error)

	//Commits messages if any
	//This func will commit the message consumed
	//by all the previous calls to GetMessages
	Commit()
}
