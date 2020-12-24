package messagebroker

import (
	"time"
)

// request models for messagebroker
type CreateTopicRequest struct {
	Name          string
	NumPartitions int
}

type DeleteTopicRequest struct {
	Name string
}

type SendMessageToTopicRequest struct {
	Topic      string
	Message    []byte
	Attributes []map[string][]byte
}

type GetMessagesFromTopicRequest struct {
	NumOfMessages int
	Timeout       time.Duration
}

type CreateTopicResponse struct {
}

type DeleteTopicResponse struct {
}

type SendMessageToTopicResponse struct {
	MessageId string
}

type GetMessagesFromTopicResponse struct {
	Messages []string
}

type CommitOnTopicResponse struct {
}
