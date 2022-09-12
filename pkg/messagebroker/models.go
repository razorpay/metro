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

// ModifyTopicConfigRequest ...
type ModifyTopicConfigRequest struct {
	Name   string
	Config map[string]string
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
	// the unique identifier of each broker message
	MessageID string
	// time at which the message was pushed onto the broker
	PublishTime time.Time
	// the first topic from where the message originated from
	// the message eventually can cycle through multiple delay and dead-letter topics
	SourceTopic string
	// the current retry topic through which the message is read from
	RetryTopic string
	// the subscription name from where this message originated
	Subscription string
	// the number of retries already attempted for the current message
	CurrentRetryCount int32
	// max attempted based on the dead-letter policy configured in the subscription
	MaxRetryCount int32
	// the current topic where the message is being read from
	CurrentTopic string
	// the delay with which retry began
	InitialDelayInterval uint
	// the current delay calculated being after applying backoff
	CurrentDelayInterval uint
	// the actual interval being used from the pre-defined list of intervals
	ClosestDelayInterval uint
	// the dead letter topic name where is message will be pushed after exhausted all retries
	DeadLetterTopic string
	// the time interval after which this message can be read by the delay-consumer on a retry cycle
	NextDeliveryTime time.Time
	// Sequence number for ordered message delivery
	CurrentSequence int32
	// sequence number of the previous message in ordered message delivery
	PrevSequence int32
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
			"currentDelayInterval": mh.CurrentDelayInterval,
			"closestDelayInterval": mh.ClosestDelayInterval,
			"deadLetterTopic":      mh.DeadLetterTopic,
			"nextDeliveryTime":     mh.NextDeliveryTime.Unix(),
			"currentSequence":      mh.CurrentSequence,
			"prevSequence":         mh.PrevSequence,
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
	Messages []ReceivedMessage
}

// HasNonZeroMessages ...
func (g *GetMessagesFromTopicResponse) HasNonZeroMessages() bool {
	return len(g.Messages) > 0
}

// ReceivedMessage ...
type ReceivedMessage struct {
	Data        []byte
	Topic       string
	Partition   int32
	Offset      int32
	OrderingKey string
	Attributes  []map[string][]byte
	MessageHeader
}

// HasReachedRetryThreshold ...
func (rm ReceivedMessage) HasReachedRetryThreshold() bool {
	return rm.CurrentRetryCount >= rm.MaxRetryCount
}

// CanProcessMessage a message can be processed only:
// if current current time is greater than the next delivery time OR
// the number of allowed retries have been exhausted
func (rm ReceivedMessage) CanProcessMessage() bool {
	return time.Now().Unix() >= rm.NextDeliveryTime.Unix() || rm.HasReachedRetryThreshold()
}

// RequiresOrdering checks if the message contains an ordering key
func (rm ReceivedMessage) RequiresOrdering() bool {
	return rm.OrderingKey != ""
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
