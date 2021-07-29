package messagebroker

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

// IsErrorRecoverable ...
func IsErrorRecoverable(err error) bool {

	if err == nil {
		return true
	}

	switch err.(type) {
	case kafkapkg.Error:
		kErr := err.(kafkapkg.Error)
		return kErr.IsRetriable() || retryableKafkaErrorCodes[kErr.Code()]
	default:
		return false
	}
	return false
}
