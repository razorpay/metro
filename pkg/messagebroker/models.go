package messagebroker

import "encoding/json"

// CreateTopicRequest ...
type CreateTopicRequest struct {
	Name          string
	NumPartitions int
}

// DeleteTopicRequest ...
type DeleteTopicRequest struct {
	Name           string
	Force          bool //  only required for pulsar and ignored for kafka
	NonPartitioned bool //  only required for pulsar and ignored for kafka
}

// SendMessageToTopicRequest ...
type SendMessageToTopicRequest struct {
	Topic      string
	Message    []byte
	Attributes []map[string][]byte
	TimeoutSec int
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
	ID        string
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
	Response  interface{}
}

// GetMessagesFromTopicResponse ...
type GetMessagesFromTopicResponse struct {
	OffsetWithMessages map[string]string
	Response           interface{}
}

// CommitOnTopicResponse ...
type CommitOnTopicResponse struct {
	Response interface{}
}

// GetTopicMetadataResponse ...
type GetTopicMetadataResponse struct {
	Response interface{}
}

type pulsarAckMessage struct {
	ID string
}

func (pm *pulsarAckMessage) Serialize() []byte {
	bytes, _ := json.Marshal(pm.ID)
	return bytes
}
