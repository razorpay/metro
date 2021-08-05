package subscriber

import (
	"container/heap"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/razorpay/metro/pkg/utils"

	"github.com/razorpay/metro/internal/subscriber/customheap"
	"github.com/razorpay/metro/pkg/messagebroker"
)

// PullRequest ...
type PullRequest struct {
	ctx              context.Context
	MaxNumOfMessages int32
}

// WithContext can be used to set the current context to the request
func (req *PullRequest) WithContext(ctx context.Context) *PullRequest {
	req.ctx = ctx
	return req
}

// AcknowledgeRequest ...
type AcknowledgeRequest struct {
	AckIDs []string
}

// IsEmpty returns true if its an empty request
func (ar *AcknowledgeRequest) IsEmpty() bool {
	if len(ar.AckIDs) == 0 {
		return true
	}
	return false
}

// ModifyAckDeadlineRequest ...
type ModifyAckDeadlineRequest struct {
	// The initial ACK deadline given to messages is 10s
	// https://godoc.org/cloud.google.com/go/pubsub#hdr-Deadlines
	ModifyDeadlineSeconds []int32
	ModifyDeadlineAckIDs  []string
}

// IsEmpty returns true if its an empty request
func (mr *ModifyAckDeadlineRequest) IsEmpty() bool {
	if len(mr.ModifyDeadlineAckIDs) == 0 {
		return true
	}
	return false
}

// IAckMessage ...
type IAckMessage interface {
	BuildAckID() string

	// return true if ack_id has originated from this server
	MatchesOriginatingMessageServer() bool
}

// AckMessage ...
type AckMessage struct {
	ctx           context.Context
	ServerAddress string
	SubscriberID  string
	Topic         string
	Partition     int32
	Offset        int32
	MessageID     string
	Deadline      int32
	AckID         string
}

const ackIDSeparator = "_"

// NewAckMessage ...
func NewAckMessage(subscriberID, topic string, partition, offset, deadline int32, messageID string) IAckMessage {
	// TODO: add needed validations on all fields
	return &AckMessage{
		SubscriberID: subscriberID,
		Topic:        topic,
		Partition:    partition,
		MessageID:    messageID,
		Deadline:     deadline,
		Offset:       offset,
	}
}

// WithContext can be used to set the current context to the request
func (a *AckMessage) WithContext(ctx context.Context) *AckMessage {
	a.ctx = ctx
	return a
}

func (a *AckMessage) String() string {
	return fmt.Sprintf("serverAddress:[%v], subscriberId:[%v], topic:[%v], partition:[%v], offset:[%v], msgId:[%v], deadline:[%v], ack_id:[%v]",
		a.ServerAddress, a.SubscriberID, a.Topic, a.Partition, a.Offset, a.MessageID, a.Deadline, a.AckID)
}

// BuildAckID ...
func (a *AckMessage) BuildAckID() string {
	builder := strings.Builder{}

	// append server host
	builder.WriteString(utils.Encode(currentHostIP))
	builder.WriteString(ackIDSeparator)

	// append subscriber id
	builder.WriteString(utils.Encode(a.SubscriberID))
	builder.WriteString(ackIDSeparator)

	// append topic name
	builder.WriteString(utils.Encode(a.Topic))
	builder.WriteString(ackIDSeparator)

	// append topic partition
	builder.WriteString(utils.Encode(fmt.Sprintf("%v", a.Partition)))
	builder.WriteString(ackIDSeparator)

	// append partition offset
	builder.WriteString(utils.Encode(fmt.Sprintf("%v", a.Offset)))
	builder.WriteString(ackIDSeparator)

	// append ack deadline
	builder.WriteString(utils.Encode(fmt.Sprintf("%v", a.Deadline)))
	builder.WriteString(ackIDSeparator)

	// append message id
	builder.WriteString(utils.Encode(a.MessageID))

	a.AckID = builder.String()

	return builder.String()
}

// MatchesOriginatingMessageServer ...
func (a *AckMessage) MatchesOriginatingMessageServer() bool {
	return currentHostIP == a.ServerAddress
}

// HasHitDeadline ...
func (a *AckMessage) HasHitDeadline() bool {
	return time.Now().Unix() > int64(a.Deadline)
}

// ParseAckID ...
func ParseAckID(ackID string) *AckMessage {
	// split the message and parse tokens
	parts := strings.Split(ackID, ackIDSeparator)

	// TODO : add validations
	partition, _ := strconv.ParseInt(utils.Decode(parts[3]), 10, 0)
	offset, _ := strconv.ParseInt(utils.Decode(parts[4]), 10, 0)
	deadline, _ := strconv.ParseInt(utils.Decode(parts[5]), 10, 0)

	return &AckMessage{
		ServerAddress: utils.Decode(parts[0]),
		SubscriberID:  utils.Decode(parts[1]),
		Topic:         utils.Decode(parts[2]),
		Partition:     int32(partition),
		Offset:        int32(offset),
		Deadline:      int32(deadline),
		MessageID:     utils.Decode(parts[6]),
		AckID:         ackID,
	}
}

var currentHostIP string

func init() {
	lookupAndSetIP()
}

func lookupAndSetIP() {
	env := os.Getenv("APP_ENV")
	if env == "dev_docker" || env == "" {
		currentHostIP = "1.2.3.4"
		return
	}

	// TODO: check if this works on pods
	host, _ := os.Hostname()
	addrs, _ := net.LookupIP(host)
	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			currentHostIP = ipv4.String()
			break
		}
	}

	if currentHostIP == "" {
		panic("failed to lookup host ip")
	}
}

// ToTopicPartition ...
func (a *AckMessage) ToTopicPartition() TopicPartition {
	return TopicPartition{
		topic:     a.Topic,
		partition: a.Partition,
	}
}

// ConsumptionMetadata ...
type ConsumptionMetadata struct {
	// data structures to hold messages in-memory
	consumedMessages              map[string]interface{} // hold all consumed messages. this will help throttle based on maxOutstandingMessages and maxOutstandingBytes
	offsetBasedMinHeap            customheap.OffsetBasedPriorityQueue
	deadlineBasedMinHeap          customheap.DeadlineBasedPriorityQueue
	maxCommittedOffset            int32          // our counter will init to that value initially
	evictedButNotCommittedOffsets map[int32]bool // holds all offsets which have been evicted from the heap but not yet committed to the broker
}

// NewConsumptionMetadata ...
func NewConsumptionMetadata() *ConsumptionMetadata {
	cm := &ConsumptionMetadata{
		consumedMessages:              make(map[string]interface{}),
		offsetBasedMinHeap:            customheap.NewOffsetBasedPriorityQueue(),
		deadlineBasedMinHeap:          customheap.NewDeadlineBasedPriorityQueue(),
		maxCommittedOffset:            0,
		evictedButNotCommittedOffsets: make(map[int32]bool),
	}

	// init the heaps as well
	heap.Init(&cm.offsetBasedMinHeap)
	heap.Init(&cm.deadlineBasedMinHeap)

	return cm
}

// Store updates all the internal data structures with the consumed message metadata
func (cm *ConsumptionMetadata) Store(msg messagebroker.ReceivedMessage, deadline int64) {
	cm.consumedMessages[msg.MessageID] = msg

	msg1 := &customheap.AckMessageWithOffset{
		MsgID:  msg.MessageID,
		Offset: msg.Offset,
	}
	cm.offsetBasedMinHeap.Indices = append(cm.offsetBasedMinHeap.Indices, msg1)
	cm.offsetBasedMinHeap.MsgIDToIndexMapping[msg.MessageID] = len(cm.offsetBasedMinHeap.Indices) - 1
	heap.Init(&cm.offsetBasedMinHeap)

	msg2 := &customheap.AckMessageWithDeadline{
		MsgID:       msg.MessageID,
		AckDeadline: int32(deadline),
	}
	cm.deadlineBasedMinHeap.Indices = append(cm.deadlineBasedMinHeap.Indices, msg2)
	cm.deadlineBasedMinHeap.MsgIDToIndexMapping[msg.MessageID] = len(cm.deadlineBasedMinHeap.Indices) - 1
	heap.Init(&cm.deadlineBasedMinHeap)
}

// TopicPartition ...
type TopicPartition struct {
	topic     string
	partition int32
}

// NewTopicPartition ...
func NewTopicPartition(topic string, partition int32) TopicPartition {
	return TopicPartition{
		topic:     topic,
		partition: partition,
	}
}

func (tp TopicPartition) String() string {
	return fmt.Sprintf("%v-%v", tp.topic, tp.partition)
}

// ModAckMessage ...
type ModAckMessage struct {
	ctx         context.Context
	AckMessage  *AckMessage
	ackDeadline int32
}

// WithContext can be used to set the current context to the request
func (a *ModAckMessage) WithContext(ctx context.Context) *ModAckMessage {
	a.ctx = ctx
	return a
}

// String ...
func (a *ModAckMessage) String() string {
	return fmt.Sprintf("ackMessage:[%v], ackDeadline:[%v]", a.AckMessage, a.ackDeadline)
}

// NewModAckMessage ...
func NewModAckMessage(ackMessage *AckMessage, ackDeadline int32) *ModAckMessage {
	return &ModAckMessage{
		AckMessage:  ackMessage,
		ackDeadline: ackDeadline,
	}
}

// RetryMessage ...
type RetryMessage struct {
	Data       []byte
	Topic      string
	Partition  int32
	Offset     int32
	MessageID  string
	RetryCount int32
}

// increments the retry attempt
func (rm *RetryMessage) incrementAndGetRetryCount() int32 {
	rm.RetryCount++
	return rm.RetryCount
}

// NewRetryMessage ...
func NewRetryMessage(topic string, partition, offset int32, data []byte, messageID string, retryCount int32) *RetryMessage {
	return &RetryMessage{
		Data:       data,
		Topic:      topic,
		Partition:  partition,
		Offset:     offset,
		MessageID:  messageID,
		RetryCount: retryCount,
	}
}
