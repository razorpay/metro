package messagebroker

import (
	"encoding/json"
	"strings"
	"time"

	kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/xid"
)

const (
	// below are the kafka header keys
	messageIDHeader            = "messageID"
	publishTimeHeader          = "publishTime"
	sourceTopicHeader          = "sourceTopic"
	retryTopicHeader           = "retryTopic"
	currentTopicHeader         = "currentTopic"
	subscriptionHeader         = "subscription"
	currentRetryCountHeader    = "currentRetryCount"
	maxRetryCountHeader        = "maxRetryCount"
	initialDelayIntervalHeader = "initialDelayInterval"
	deadLetterTopicHeader      = "deadLetterTopic"
	nextDeliveryTimeHeader     = "nextDeliveryTime"
)

// extracts the message headers from a given SendMessageToTopicRequest and converts to the equivalent broker headers
func convertRequestToKafkaHeaders(request SendMessageToTopicRequest) []kafkapkg.Header {
	kHeaders := make([]kafkapkg.Header, 0)
	// extract default attributes
	if request.Attributes != nil {
		for _, attribute := range request.Attributes {
			for k, v := range attribute {
				kHeaders = append(kHeaders, kafkapkg.Header{
					Key:   k,
					Value: v,
				})
			}
		}
	}
	// extract messageID
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   messageIDHeader,
		Value: []byte(request.MessageID),
	})
	// extract currentRetryCount
	crc, _ := json.Marshal(request.CurrentRetryCount)
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   currentRetryCountHeader,
		Value: crc,
	})
	// extract maxRetryCount
	mrc, _ := json.Marshal(request.MaxRetryCount)
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   maxRetryCountHeader,
		Value: mrc,
	})
	// extract publishTime
	pt, _ := json.Marshal(time.Now().Unix()) // set current time as publish time
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   publishTimeHeader,
		Value: pt,
	})
	// extract sourceTopic
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   sourceTopicHeader,
		Value: []byte(request.SourceTopic),
	})
	// extract retryTopic
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   retryTopicHeader,
		Value: []byte(request.RetryTopic),
	})
	// extract currentTopic
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   currentTopicHeader,
		Value: []byte(request.CurrentTopic),
	})
	// extract subscription
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   subscriptionHeader,
		Value: []byte(request.Subscription),
	})
	// extract initialDelayInterval
	idi, _ := json.Marshal(request.InitialDelayInterval)
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   initialDelayIntervalHeader,
		Value: idi,
	})
	// extract deadLetterTopic
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   deadLetterTopicHeader,
		Value: []byte(request.DeadLetterTopic),
	})
	// extract nextDeliveryTime if sent
	ndt, _ := json.Marshal(request.NextDeliveryTime.Unix())
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   nextDeliveryTimeHeader,
		Value: ndt,
	})

	return kHeaders
}

// extracts the message headers from a given broker message and populates the needed response struct
func convertKafkaHeadersToResponse(headers []kafkapkg.Header) ReceivedMessage {

	var (
		messageID            string
		publishTime          int64 // unix timestamp
		sourceTopic          string
		retryTopic           string
		currentTopic         string
		subscription         string
		currentRetryCount    int32
		maxRetryCount        int32
		initialDelayInterval uint
		deadLetterTopic      string
		nextDeliveryTime     int64 // unix timestamp
		otherAttributes      []map[string][]byte
	)
	for _, v := range headers {
		switch v.Key {
		case messageIDHeader:
			messageID = string(v.Value)
		case publishTimeHeader:
			json.Unmarshal(v.Value, &publishTime)
		case sourceTopicHeader:
			sourceTopic = string(v.Value)
		case retryTopicHeader:
			retryTopic = string(v.Value)
		case subscriptionHeader:
			subscription = string(v.Value)
		case currentRetryCountHeader:
			json.Unmarshal(v.Value, &currentRetryCount)
		case maxRetryCountHeader:
			json.Unmarshal(v.Value, &maxRetryCount)
		case currentTopicHeader:
			currentTopic = string(v.Value)
		case initialDelayIntervalHeader:
			json.Unmarshal(v.Value, &initialDelayInterval)
		case deadLetterTopicHeader:
			deadLetterTopic = string(v.Value)
		case nextDeliveryTimeHeader:
			json.Unmarshal(v.Value, &nextDeliveryTime)
		default:
			otherAttributes = append(otherAttributes, map[string][]byte{
				v.Key: v.Value,
			})
		}
	}

	return ReceivedMessage{
		Attributes: otherAttributes,
		MessageHeader: MessageHeader{
			MessageID:            messageID,
			PublishTime:          time.Unix(publishTime, 0),
			SourceTopic:          sourceTopic,
			RetryTopic:           retryTopic,
			CurrentTopic:         currentTopic,
			Subscription:         subscription,
			CurrentRetryCount:    currentRetryCount,
			InitialDelayInterval: initialDelayInterval,
			MaxRetryCount:        maxRetryCount,
			DeadLetterTopic:      deadLetterTopic,
			NextDeliveryTime:     time.Unix(nextDeliveryTime, 0),
		},
	}
}

func getMessageID(messageID string) string {
	if messageID == "" {
		// generate a message id and attach only if not sent by the caller
		// in case of retry push to topic, the same messageID is to be re-used
		messageID = xid.New().String()
	}
	return messageID
}

// normalizeTopicName returns the actual topic name used in message broker
func normalizeTopicName(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}
