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
	panic("implement me")
}

func (p PulsarBroker) DeleteTopic(topic string) error {
	panic("implement me")
}

func (p PulsarBroker) Produce(topic string, message []byte) ([]string, error) {
	panic("implement me")
}

func (p PulsarBroker) GetMessages(numOfMessages int, timeout time.Duration) ([]string, error) {
	panic("implement me")
}

func (p PulsarBroker) Commit() {
	panic("implement me")
}

func NewPulsarBroker(ctx context.Context, bConfig *BrokerConfig) Broker {
	return &PulsarBroker{}
}
