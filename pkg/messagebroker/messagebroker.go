package messagebroker

import "time"

// Broker interface for all types of ops
type Broker interface {
	Admin
	Producer
	Consumer
}

// Admin for admin operations on topics, partitions, updating schema registry etc
type Admin interface {
	CreateTopic(topic string) error

	DeleteTopic(topic string) error
}

// Producer for produce operations
type Producer interface {
	Produce(topic string, message []byte) (string, error)
}

// Consumer interface for consuming messages
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
