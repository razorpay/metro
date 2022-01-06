package subscriber

import (
	"context"
	"fmt"
	"testing"

	"github.com/razorpay/metro/internal/subscription"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber_MessageFiltering(t *testing.T) {
	ctx := context.Background()

	messages := []*metrov1.ReceivedMessage{
		{
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"domain": "com",
				},
				MessageId: "1",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"domain": "com",
					"x":      "org",
				},
				MessageId: "2",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"domain": "org",
				},
				MessageId: "3",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"x": "company",
				},
				MessageId: "4",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"x": "dotcom",
				},
				MessageId: "5",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"x": "org",
				},
				MessageId: "6",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				MessageId: "7",
			},
		},
	}

	tests := []struct {
		FilterExpression   string
		FilteredMessageIDs map[string]interface{}
	}{
		{
			FilterExpression:   "attributes:domain",
			FilteredMessageIDs: map[string]interface{}{"1": true, "2": true, "3": true},
		},
		{
			FilterExpression:   "attributes:domain AND attributes.x = \"org\"",
			FilteredMessageIDs: map[string]interface{}{"2": true},
		},
		{
			FilterExpression:   "hasPrefix(attributes.domain, \"co\") OR hasPrefix(attributes.x, \"co\")",
			FilteredMessageIDs: map[string]interface{}{"1": true, "2": true, "4": true},
		},
		{
			FilterExpression:   "(attributes:domain AND attributes.domain = \"com\") OR (attributes:x AND NOT hasPrefix(attributes.x,\"co\"))",
			FilteredMessageIDs: map[string]interface{}{"1": true, "2": true, "5": true, "6": true},
		},
	}

	for _, test := range tests {
		mockImpl := &BasicImplementation{
			subscription: &subscription.Model{
				Name:             "test-sub",
				Topic:            "test-topic",
				FilterExpression: test.FilterExpression,
			},
		}
		filteredMessages := filterMessages(ctx, mockImpl, messages, make(chan error))
		expectedMap := test.FilteredMessageIDs
		for _, msg := range filteredMessages {
			assert.NotNil(t, expectedMap[msg.Message.MessageId], fmt.Sprintf("Filter: %s, MesageID: %s", test.FilterExpression, msg.Message.MessageId))
			delete(expectedMap, msg.Message.MessageId)
		}
		assert.Empty(t, expectedMap)
	}

}
