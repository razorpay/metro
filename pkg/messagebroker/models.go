package messagebroker

import (
	"encoding/json"
	"fmt"
	"time"
)

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
	Topic       string
	Message     []byte
	OrderingKey string
	Attributes  []map[string][]byte
	TimeoutSec  int
}

// GetMessagesFromTopicRequest ...
type GetMessagesFromTopicRequest struct {
	NumOfMessages int32
	TimeoutSec    int
}

// CommitOnTopicRequest ...
type CommitOnTopicRequest struct {
	Topic     string
	Partition int32
	Offset    int32
	ID        string
}

// GetTopicMetadataRequest ...
type GetTopicMetadataRequest struct {
	Topic     string
	Partition int32
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
	Topic       string
	Partition   int32
	Offset      int32
	PublishTime time.Time
}

func (rm ReceivedMessage) String() string {
	return fmt.Sprintf("data=[%v], msgId=[%v], partition=[%v], offset=[%v], publishTime=[%v]",
		string(rm.Data), rm.MessageID, rm.Partition, rm.Offset, rm.PublishTime.Unix())
}

// CommitOnTopicResponse ...
type CommitOnTopicResponse struct {
	Response interface{}
}

// GetTopicMetadataResponse ...
type GetTopicMetadataResponse struct {
	Topic     string
	Partition int32
	Offset    int32
}

// PauseOnTopicRequest ...
type PauseOnTopicRequest struct {
	Topic     string
	Partition int32
}

// ResumeOnTopicRequest ...
type ResumeOnTopicRequest struct {
	Topic     string
	Partition int32
}

type pulsarAckMessage struct {
	ID string
}

func (pm *pulsarAckMessage) Serialize() []byte {
	bytes, _ := json.Marshal(pm.ID)
	return bytes
}
