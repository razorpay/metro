package messagebroker

import (
	"encoding/json"
	"time"

	kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/xid"
)

const (
	// below are the kafka header keys
	messageIDHeader         = "messageID"
	publishTimeHeader       = "publishTime"
	sourceTopicHeader       = "sourceTopic"
	subscriptionHeader      = "subscription"
	currentRetryCountHeader = "currentRetryCount"
	maxRetryCountHeader     = "maxRetryCount"
	nextTopicHeader         = "nextTopic"
	nextDeliveryTimeHeader  = "nextDeliveryTime"
)

// extracts the message headers from a given SendMessageToTopicRequest and converts to the equivalent broker headers
func convertRequestToKafkaHeaders(request SendMessageToTopicRequest) []kafkapkg.Header {
	var kHeaders []kafkapkg.Header
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
	// extract subscription
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   subscriptionHeader,
		Value: []byte(request.Subscription),
	})
	// extract nextTopic
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   nextTopicHeader,
		Value: []byte(request.NextTopic),
	})
	// extract nextDeliveryTime if sent
	if !request.PublishTime.IsZero() {
		ndt, _ := json.Marshal(request.NextDeliveryTime.Unix())
		kHeaders = append(kHeaders, kafkapkg.Header{
			Key:   nextDeliveryTimeHeader,
			Value: ndt,
		})
	}

	return kHeaders
}

// extracts the message headers from a given broker message and populates the needed response struct
func convertKafkaHeadersToResponse(headers []kafkapkg.Header) ReceivedMessage {

	var (
		messageID         string
		publishTime       time.Time
		sourceTopic       string
		subscription      string
		currentRetryCount int32
		maxRetryCount     int32
		nextTopic         string
		nextDeliveryTime  time.Time
	)
	for _, v := range headers {
		switch v.Key {
		case messageIDHeader:
			messageID = string(v.Value)
		case publishTimeHeader:
			json.Unmarshal(v.Value, &publishTime)
		case sourceTopicHeader:
			sourceTopic = string(v.Value)
		case subscriptionHeader:
			subscription = string(v.Value)
		case currentRetryCountHeader:
			json.Unmarshal(v.Value, &currentRetryCount)
		case maxRetryCountHeader:
			json.Unmarshal(v.Value, &maxRetryCount)
		case nextTopicHeader:
			nextTopic = string(v.Value)
		case nextDeliveryTimeHeader:
			json.Unmarshal(v.Value, &nextDeliveryTime)
		}
	}

	return ReceivedMessage{
		MessageHeader: MessageHeader{
			MessageID:         messageID,
			PublishTime:       publishTime,
			SourceTopic:       sourceTopic,
			Subscription:      subscription,
			CurrentRetryCount: currentRetryCount,
			MaxRetryCount:     maxRetryCount,
			NextTopic:         nextTopic,
			NextDeliveryTime:  nextDeliveryTime,
		},
	}
}
func getMessageId(messageID string) string {
	if messageID == "" {
		// generate a message id and attach only if not sent by the caller
		// in case of retry push to topic, the same messageID is to be re-used
		messageID = xid.New().String()
	}
	return messageID
}
