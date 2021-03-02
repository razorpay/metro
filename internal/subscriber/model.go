package subscriber

import (
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

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
	AckID         string
}

const ackIDSeparator = "_"

// NewAckMessage ...
func NewAckMessage(subscriberID, topic string, partition, offset int32, messageID string) IAckMessage {
	// TODO: add needed validations on all fields
	return &AckMessage{
		SubscriberID: subscriberID,
		Topic:        topic,
		Partition:    partition,
		MessageID:    messageID,
	}
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

	return &AckMessage{
		ServerAddress: decode(parts[0]),
		SubscriberID:  decode(parts[1]),
		Topic:         decode(parts[2]),
		Partition:     int32(partition),
		Offset:        int32(offset),
		MessageID:     decode(parts[5]),
		AckID:         ackID,
	}
}

var currentHostIP string

func init() {
	lookupAndSetIP()
}

func lookupAndSetIP() {
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
