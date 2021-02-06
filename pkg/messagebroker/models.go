package messagebroker

import "time"

// CreateTopicRequest ...
type CreateTopicRequest struct {
	Name          string
	NumPartitions int
}

// DeleteTopicRequest ...
type DeleteTopicRequest struct {
	Name string
}

// SendMessageToTopicRequest ...
type SendMessageToTopicRequest struct {
	Topic       string
	Message     []byte
	OrderingKey string
	Attributes  []map[string][]byte
	TimeoutSec  int
}

// GetMessagesFromTopicRequest ...
type GetMessagesFromTopicRequest struct {
	NumOfMessages int
	TimeoutSec    int
}

// CommitOnTopicRequest ...
type CommitOnTopicRequest struct {
	Topic     string
	Partition int32
	Offset    int64
}

// GetTopicMetadataRequest ...
type GetTopicMetadataRequest struct {
	Topic      string
	TimeoutSec int
}

// CreateTopicResponse ...
type CreateTopicResponse struct {
	Response interface{}
}

// DeleteTopicResponse ...
type DeleteTopicResponse struct {
	Response interface{}
}

// SendMessageToTopicResponse ...
type SendMessageToTopicResponse struct {
	MessageID string
}

// GetMessagesFromTopicResponse ...
type GetMessagesFromTopicResponse struct {
	OffsetWithMessages map[string]ReceivedMessage
}

// ReceivedMessage ...
type ReceivedMessage struct {
	Data        []byte
	MessageID   string
	PublishTime time.Time
}

// CommitOnTopicResponse ...
type CommitOnTopicResponse struct {
	Response interface{}
}

// GetTopicMetadataResponse ...
type GetTopicMetadataResponse struct {
	Response interface{}
}
