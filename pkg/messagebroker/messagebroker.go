package messagebroker

import "context"

// Broker interface for all types of ops
type Broker interface {
	Admin
	Producer
	Consumer
}

// Admin for admin operations on topics, partitions, updating schema registry etc
type Admin interface {
	// creates a new topic if not available
	CreateTopic(context.Context, CreateTopicRequest) (CreateTopicResponse, error)

	// deletes an existing topic
	DeleteTopic(context.Context, DeleteTopicRequest) (DeleteTopicResponse, error)
}

// Producer for produce operations
type Producer interface {
	// sends a message on the topic
	SendMessage(context.Context, SendMessageToTopicRequest) (*SendMessageToTopicResponse, error)
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
	CommitByPartitionAndOffset(context.Context, CommitOnTopicRequest) (CommitOnTopicResponse, error)

	// Commits a message by ID
	CommitByMsgID(context.Context, CommitOnTopicRequest) (CommitOnTopicResponse, error)

	GetTopicMetadata(context.Context, GetTopicMetadataRequest) (GetTopicMetadataResponse, error)

	// pause the consumer
	Pause(context.Context, PauseOnTopicRequest) error

	// resume the consumer
	Resume(context.Context, ResumeOnTopicRequest) error

	// closes the consumer
	Close(context.Context) error
}
