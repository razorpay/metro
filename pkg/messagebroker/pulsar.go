package messagebroker

import (
	"context"
	"time"
)

type PulsarBroker struct {
	Ctx    context.Context
	Config *BrokerConfig
}

func (p PulsarBroker) CreateTopic(topic string) error {
	//TODO: Implement me
	return nil
}

func (p PulsarBroker) DeleteTopic(topic string) error {
	//TODO: Implement me
	return nil
}

func (p PulsarBroker) Produce(topic string, message []byte) (string, error) {
	//TODO: Implement me
	return "", nil
}

func (p PulsarBroker) GetMessages(numOfMessages int, timeout time.Duration) ([]string, error) {
	//TODO: Implement me
	return nil, nil
}

func (p PulsarBroker) Commit() {
	//TODO: Implement me
}

func NewPulsarBroker(ctx context.Context, bConfig *BrokerConfig) (Broker, error) {
	return &PulsarBroker{}, nil
}
