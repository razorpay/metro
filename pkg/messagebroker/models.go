package messagebroker

import (
	"time"
)

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
	Topic      string
	Message    []byte
	Attributes []map[string][]byte
	Timeout    time.Duration
}

// GetMessagesFromTopicRequest ...
type GetMessagesFromTopicRequest struct {
	NumOfMessages int
	Timeout       time.Duration
}

// CommitOnTopicRequest ...
type CommitOnTopicRequest struct {
	Topic     string
	Partition int32
	Offset    int64
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
	Response  interface{}
}

// GetMessagesFromTopicResponse ...
type GetMessagesFromTopicResponse struct {
	Messages []string
	Response interface{}
}

// CommitOnTopicResponse ...
type CommitOnTopicResponse struct {
	Response interface{}
}
