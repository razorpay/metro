package stream

import kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"

var retryableKafkaErrorCodes map[kafkapkg.ErrorCode]bool

func init() {
	retryableKafkaErrorCodes = map[kafkapkg.ErrorCode]bool{
		kafkapkg.ErrTimedOut:        true,
		kafkapkg.ErrMsgTimedOut:     true,
		kafkapkg.ErrRequestTimedOut: true,
		// add more as identified in future
	}
}

func isErrorRecoverable(err error) bool {

	if err == nil {
		return true
	}

	switch err.(type) {
	case kafkapkg.Error:
		kErr := err.(kafkapkg.Error)
		if kErr.IsRetriable() {
			return true
		}
		return retryableKafkaErrorCodes[kErr.Code()]
	default:
		return false
	}
	return false
}
