package worker

import "github.com/razorpay/metro/pkg/messagebroker"

type pushsubscriber struct {
	SubcriptionKey string
	Broker         string
	BrokerConfig   messagebroker.BrokerConfig
}

// Start reads the messages from the broker and publish them to the subscription endpoint
func (subscriber *pushsubscriber) Start() error {
	// Read from broker and publish to endpoint
	return nil
}

// Stop is used to terminate the push subscription processing
func (subscriber *pushsubscriber) Stop() error {
	// Stop the pushsubscription
	return nil
}
