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
}

// GetMessagesFromTopicRequest ...
type GetMessagesFromTopicRequest struct {
	NumOfMessages int
	Timeout       time.Duration
}

// CreateTopicResponse ...
type CreateTopicResponse struct {
}

// DeleteTopicResponse ...
type DeleteTopicResponse struct {
}

// SendMessageToTopicResponse ...
type SendMessageToTopicResponse struct {
	MessageID string
}

// GetMessagesFromTopicResponse ...
type GetMessagesFromTopicResponse struct {
	Messages []string
}

// CommitOnTopicResponse ...
type CommitOnTopicResponse struct {
}
