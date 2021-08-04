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

// AddTopicPartitionRequest ...
type AddTopicPartitionRequest struct {
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
	TimeoutMs   int
	Attributes  []map[string][]byte
	MessageHeader
}

// MessageHeader contains the fields passed around in the message headers
type MessageHeader struct {
	MessageID   string
	PublishTime time.Time
	// message was read from this topic
	SourceTopic string
	// primary retry topic
	RetryTopic string
	// topic from where the first message originated from, before any retry
	Subscription      string
	CurrentRetryCount int32
	MaxRetryCount     int32
	// destination topic
	CurrentTopic         string
	InitialDelayInterval uint
	DeadLetterTopic      string
	NextDeliveryTime     time.Time
}

// LogFields ...
func (mh MessageHeader) LogFields() []interface{} {
	return []interface{}{
		"messageHeader", map[string]interface{}{
			"messageID":            mh.MessageID,
			"publishTime":          mh.PublishTime.Unix(),
			"sourceTopic":          mh.SourceTopic,
			"retryTopic":           mh.RetryTopic,
			"subscription":         mh.Subscription,
			"currentRetryCount":    mh.CurrentRetryCount,
			"maxRetryCount":        mh.MaxRetryCount,
			"currentTopic":         mh.CurrentTopic,
			"initialDelayInterval": mh.InitialDelayInterval,
			"deadLetterTopic":      mh.DeadLetterTopic,
			"nextDeliveryTime":     mh.NextDeliveryTime.Unix(),
		},
	}
}

// GetMessagesFromTopicRequest ...
type GetMessagesFromTopicRequest struct {
	NumOfMessages int32
	TimeoutMs     int
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

// AddTopicPartitionResponse ...
type AddTopicPartitionResponse struct {
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

// PartitionOffset ...
type PartitionOffset struct {
	Partition int32
	Offset    int32
}

func (po PartitionOffset) String() string {
	return fmt.Sprintf("[%v]-[%v]", po.Partition, po.Offset)
}

// NewPartitionOffset ...
func NewPartitionOffset(partition, offset int32) PartitionOffset {
	return PartitionOffset{
		Partition: partition,
		Offset:    offset,
	}
}

// GetMessagesFromTopicResponse ...
type GetMessagesFromTopicResponse struct {
	PartitionOffsetWithMessages map[string]ReceivedMessage
}

// HasNonZeroMessages ...
func (g *GetMessagesFromTopicResponse) HasNonZeroMessages() bool {
	return len(g.PartitionOffsetWithMessages) > 0
}

// ReceivedMessage ...
type ReceivedMessage struct {
	Data       []byte
	Topic      string
	Partition  int32
	Offset     int32
	Attributes []map[string][]byte
	MessageHeader
}

// HasReachedRetryThreshold ...
func (rm ReceivedMessage) HasReachedRetryThreshold() bool {
	return rm.CurrentRetryCount >= rm.MaxRetryCount
}

// CanProcessMessage ...
func (rm ReceivedMessage) CanProcessMessage() bool {
	return time.Now().Unix() >= rm.NextDeliveryTime.Unix()
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
