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
	// CreateTopic creates a new topic if not available
	CreateTopic(context.Context, CreateTopicRequest) (CreateTopicResponse, error)

	// DeleteTopic deletes an existing topic
	DeleteTopic(context.Context, DeleteTopicRequest) (DeleteTopicResponse, error)

	// AddTopicPartitions adds partitions to an existing topic
	AddTopicPartitions(context.Context, AddTopicPartitionRequest) (*AddTopicPartitionResponse, error)

	// IsHealthy checks health of underlying broker
	IsHealthy(context.Context) (bool, error)
}

// Producer for produce operations
type Producer interface {
	// SendMessage sends a message on the topic
	SendMessage(context.Context, SendMessageToTopicRequest) (*SendMessageToTopicResponse, error)

	// IsClosed checks if producer has been closed
	IsClosed(context.Context) bool

	// Shutdown closes the producer
	Shutdown(context.Context)
}

// Consumer interface for consuming messages
type Consumer interface {
	// ReceiveMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
	// from the previous committed offset. If the available messages in the queue are less, returns
	// how many ever messages are available
	ReceiveMessages(context.Context, GetMessagesFromTopicRequest) (*GetMessagesFromTopicResponse, error)

	// CommitByPartitionAndOffset Commits messages if any
	// This func will commit the message consumed
	// by all the previous calls to GetMessages
	CommitByPartitionAndOffset(context.Context, CommitOnTopicRequest) (CommitOnTopicResponse, error)

	// CommitByMsgID Commits a message by ID
	CommitByMsgID(context.Context, CommitOnTopicRequest) (CommitOnTopicResponse, error)

	// GetTopicMetadata gets the topic metadata
	GetTopicMetadata(context.Context, GetTopicMetadataRequest) (GetTopicMetadataResponse, error)

	// Pause pause the consumer
	Pause(context.Context, PauseOnTopicRequest) error

	// Resume resume the consumer
	Resume(context.Context, ResumeOnTopicRequest) error

	// Close closes the consumer
	Close(context.Context) error

	// Assign an atomic set of partitions to consume
	Assign(context.Context, AssignTopicOffsetRequest) error
}
