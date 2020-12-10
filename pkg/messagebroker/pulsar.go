package messagebroker

import (
	"context"
	"time"
)

// PulsarBroker for pulsar
type PulsarBroker struct {
	Ctx    context.Context
	Config *BrokerConfig
}

// CreateTopic ...
func (p PulsarBroker) CreateTopic(topic string) error {
	//TODO: Implement me
	return nil
}

// DeleteTopic ...
func (p PulsarBroker) DeleteTopic(topic string) error {
	//TODO: Implement me
	return nil
}

// Produce messages to pulsar
func (p PulsarBroker) Produce(topic string, message []byte) (string, error) {
	//TODO: Implement me
	return "", nil
}

// GetMessages returns a list of messages
func (p PulsarBroker) GetMessages(numOfMessages int, timeout time.Duration) ([]string, error) {
	//TODO: Implement me
	return nil, nil
}

// Commit offset
func (p PulsarBroker) Commit() {
	//TODO: Implement me
}

// NewPulsarBroker returns a pulsar broker
func NewPulsarBroker(ctx context.Context, bConfig *BrokerConfig) (Broker, error) {
	return &PulsarBroker{}, nil
}
