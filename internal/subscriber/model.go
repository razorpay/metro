package subscriber

import (
	"container/heap"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/razorpay/metro/internal/subscriber/customheap"
	"github.com/razorpay/metro/pkg/messagebroker"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// PullRequest ...
type PullRequest struct {
	MaxNumOfMessages int32
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

// FromProto returns different structs for pull, ack and modack
func FromProto(req *metrov1.StreamingPullRequest) (*AcknowledgeRequest, *ModifyAckDeadlineRequest, error) {
	if len(req.ModifyDeadlineAckIds) != len(req.ModifyDeadlineSeconds) {
		return nil, nil, fmt.Errorf("length of modify_deadline_ack_ids and modify_deadline_seconds not same")
	}
	ar := &AcknowledgeRequest{
		req.AckIds,
	}
	mr := &ModifyAckDeadlineRequest{
		req.ModifyDeadlineSeconds,
		req.ModifyDeadlineAckIds,
	}
	return ar, mr, nil
}

// IAckMessage ...
type IAckMessage interface {
	BuildAckID() string

	// return true if ack_id has originated from this server
	MatchesOriginatingMessageServer() bool
}

// AckMessage ...
type AckMessage struct {
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

func (a *AckMessage) String() string {
	return fmt.Sprintf("serverAddress:[%v], subscriberId:[%v], topic:[%v], partition:[%v], offset:[%v], msgId:[%v], deadline:[%v], ack_id:[%v]",
		a.ServerAddress, a.SubscriberID, a.Topic, a.Partition, a.Offset, a.MessageID, a.Deadline, a.AckID)
}

// BuildAckID ...
func (a *AckMessage) BuildAckID() string {
	builder := strings.Builder{}

	// append server host
	builder.WriteString(encode(currentHostIP))
	builder.WriteString(ackIDSeparator)

	// append subscriber id
	builder.WriteString(encode(a.SubscriberID))
	builder.WriteString(ackIDSeparator)

	// append topic name
	builder.WriteString(encode(a.Topic))
	builder.WriteString(ackIDSeparator)

	// append topic partition
	builder.WriteString(encode(fmt.Sprintf("%v", a.Partition)))
	builder.WriteString(ackIDSeparator)

	// append partition offset
	builder.WriteString(encode(fmt.Sprintf("%v", a.Offset)))
	builder.WriteString(ackIDSeparator)

	// append ack deadline
	builder.WriteString(encode(fmt.Sprintf("%v", a.Deadline)))
	builder.WriteString(ackIDSeparator)

	// append message id
	builder.WriteString(encode(a.MessageID))

	a.AckID = builder.String()

	return builder.String()
}

// MatchesOriginatingMessageServer ...
func (a *AckMessage) MatchesOriginatingMessageServer() bool {
	return currentHostIP == a.ServerAddress
}

// ParseAckID ...
func ParseAckID(ackID string) *AckMessage {
	// split the message and parse tokens
	parts := strings.Split(ackID, ackIDSeparator)

	// TODO : add validations
	partition, _ := strconv.ParseInt(decode(parts[3]), 10, 0)
	offset, _ := strconv.ParseInt(decode(parts[4]), 10, 0)
	deadline, _ := strconv.ParseInt(decode(parts[5]), 10, 0)

	return &AckMessage{
		ServerAddress: decode(parts[0]),
		SubscriberID:  decode(parts[1]),
		Topic:         decode(parts[2]),
		Partition:     int32(partition),
		Offset:        int32(offset),
		Deadline:      int32(deadline),
		MessageID:     decode(parts[6]),
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

func encode(input string) string {
	return base64.StdEncoding.EncodeToString([]byte(input))
}

func decode(input string) string {
	decoded, _ := base64.StdEncoding.DecodeString(input)
	return string(decoded)
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
	consumedMessages     map[string]interface{} // hold all consumed messages. this will help throttle based on maxOutstandingMessages and maxOutstandingBytes
	offsetBasedMinHeap   customheap.OffsetBasedPriorityQueue
	deadlineBasedMinHeap customheap.DeadlineBasedPriorityQueue
	maxCommittedOffset   int32 // our counter will init to that value initially
	isPaused             bool
}

// NewConsumptionMetadata ...
func NewConsumptionMetadata() *ConsumptionMetadata {
	cm := &ConsumptionMetadata{
		consumedMessages:     make(map[string]interface{}),
		offsetBasedMinHeap:   customheap.NewOffsetBasedPriorityQueue(),
		deadlineBasedMinHeap: customheap.NewDeadlineBasedPriorityQueue(),
		maxCommittedOffset:   0,
	}

	// init the heaps as well
	heap.Init(&cm.offsetBasedMinHeap)
	heap.Init(&cm.deadlineBasedMinHeap)

	return cm
}

// Store updates all the internal data structures with the consumed message metadata
func (cm *ConsumptionMetadata) Store(msg *messagebroker.ReceivedMessage, deadline int64) {
	cm.consumedMessages[msg.MessageID] = msg

	msg1 := &customheap.AckMessageWithOffset{
		MsgID:  msg.MessageID,
		Offset: msg.Offset,
	}
	cm.offsetBasedMinHeap.Indices = append(cm.offsetBasedMinHeap.Indices, msg1)

	msg2 := &customheap.AckMessageWithDeadline{
		MsgID:       msg.MessageID,
		AckDeadline: int32(deadline),
	}
	cm.deadlineBasedMinHeap.Indices = append(cm.deadlineBasedMinHeap.Indices, msg2)
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
	ackMessage  *AckMessage
	ackDeadline int32
}

func (a *ModAckMessage) String() string {
	return fmt.Sprintf("ackMessage:[%v], ackDeadline:[%v]", a.ackMessage, a.ackDeadline)
}

// NewModAckMessage ...
func NewModAckMessage(ackMessage *AckMessage, ackDeadline int32) *ModAckMessage {
	return &ModAckMessage{
		ackMessage:  ackMessage,
		ackDeadline: ackDeadline,
	}
}
