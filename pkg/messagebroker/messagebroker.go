package messagebroker

import "context"

// Broker interface for all types of ops
type Broker interface {
	Admin
	Producer
	Consumer
}

// Admin for admin operations on topics, partitions, updating schema registry etc
//go:generate go run -mod=mod github.com/golang/mock/mockgen -build_flags=-mod=mod -destination=mocks/mock_admin.go -package=mocks . Admin
type Admin interface {
	// creates a new topic if not available
	CreateTopic(context.Context, CreateTopicRequest) (CreateTopicResponse, error)

	// deletes an existing topic
	DeleteTopic(context.Context, DeleteTopicRequest) (DeleteTopicResponse, error)

	GetTopicMetadata(context.Context, GetTopicMetadataRequest) (GetTopicMetadataResponse, error)
}

// Producer for produce operations
type Producer interface {
	// sends a message on the topic
	SendMessages(context.Context, SendMessageToTopicRequest) (*SendMessageToTopicResponse, error)
}

// Consumer interface for consuming messages
type Consumer interface {
	//GetMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
	//from the previous committed offset. If the available messages in the queue are less, returns
	// how many ever messages are available
	ReceiveMessages(context.Context, GetMessagesFromTopicRequest) (*GetMessagesFromTopicResponse, error)

	//Commits messages if any
	//This func will commit the message consumed
	//by all the previous calls to GetMessages
	Commit(context.Context, CommitOnTopicRequest) (CommitOnTopicResponse, error)
}
