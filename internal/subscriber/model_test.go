package subscriber

import (
	"testing"

	"github.com/razorpay/metro/internal/merror"
	"github.com/stretchr/testify/assert"
)

var (
	validAckID   = "MS4yLjMuNA==_dGVzdC1zdWI=_dGVzdC10b3BpYw==_MA==_MA==_MTAw_dGVzdC1tZXNzYWdlLWlk"
	inValidAckID = "MS4yLjMuNA==_dGVzdC1zdWI=_dGVzdC10b3BpYw==_MA==_MA=="
)

func getValidAcknowledgeMessageWithServerAddress(a *AckMessage) *AckMessage {
	a.ServerAddress = "1.2.3.4"
	return a
}

func getValidAcknowledgeMessageWithAckID(a *AckMessage) *AckMessage {
	a.AckID = validAckID
	return a
}

func getValidAcknowledgeMessage() *AckMessage {
	return &AckMessage{
		SubscriberID: "test-sub",
		Topic:        "test-topic",
		Partition:    0,
		Offset:       0,
		Deadline:     100,
		MessageID:    "test-message-id",
	}
}

func TestModel_ParseAckID(t *testing.T) {
	expectedAckMessage := getValidAcknowledgeMessage()
	expectedAckMessage = getValidAcknowledgeMessageWithServerAddress(expectedAckMessage)
	expectedAckMessage = getValidAcknowledgeMessageWithAckID(expectedAckMessage)
	ackID := validAckID
	ackMessage, err := ParseAckID(ackID)
	assert.Nil(t, err)
	assert.Equal(t, expectedAckMessage, ackMessage)
}

func TestModel_ParseAckID_InvalidInput(t *testing.T) {
	ackID := inValidAckID
	ackMessage, err := ParseAckID(ackID)
	expectedErr := merror.Newf(merror.InvalidArgument, "AckID received is not in expected format")
	assert.Nil(t, ackMessage)
	assert.Equal(t, expectedErr, err)
}

func TestModel_NewAckMessage_ValidInput(t *testing.T) {
	ackMessage, err := NewAckMessage("test-sub", "test-topic", 0, 0, 100, "test-message-id")
	expectedAckMessage := getValidAcknowledgeMessage()
	assert.Nil(t, err)
	assert.Equal(t, expectedAckMessage, ackMessage)
}

func TestModel_NewAckMessage_InvalidPartition(t *testing.T) {
	ackMessage, err := NewAckMessage("test-sub", "test-topic", -1, 0, 100, "test-message-id")
	assert.Equal(t, ErrIllegalPartitionValue, err)
	assert.Nil(t, ackMessage)
}

func TestModel_NewAckMessage_InvalidOffset(t *testing.T) {
	ackMessage, err := NewAckMessage("test-sub", "test-topic", 0, -5, 100, "test-message-id")
	assert.Nil(t, ackMessage)
	assert.Equal(t, ErrIllegalOffsetValue, err)
}

func TestModel_NewAckMessage_InvalidDeadline(t *testing.T) {
	ackMessage, err := NewAckMessage("test-sub", "test-topic", 0, 0, -100, "test-message-id")
	assert.Nil(t, ackMessage)
	assert.Equal(t, ErrIllegalDeadlineValue, err)
}
