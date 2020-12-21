package messagebroker

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"time"
)

// PulsarBroker for pulsar
type PulsarBroker struct {
	Ctx      context.Context
	Config   *BrokerConfig
	Consumer pulsar.Consumer
	Producer pulsar.Producer
}

// NewPulsarBroker returns a pulsar broker
func NewPulsarBroker(ctx context.Context, bConfig *BrokerConfig) (Broker, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               bConfig.Producer.Brokers[0],
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	var producer pulsar.Producer
	if bConfig.Producer != nil {
		p, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: "topic-1",
		})
		if err != nil {
			return nil, err
		}
		producer = p
	}

	var consumer pulsar.Consumer
	if bConfig.Consumer != nil {
		c, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            "topic-1",
			SubscriptionName: "my-subscription",
			Type:             pulsar.Shared,
		})
		if err != nil {
			return nil, err
		}
		consumer = c
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// admin client init
	// TODO : pulsar go-client does not expose Admin APIs
	// Need to write an Admin wrapper over http://pulsar.apache.org/docs/v2.0.1-incubating/reference/RestApi/
	// https://streamnative.io/en/blog/tech/2019-11-26-introduction-pulsarctl

	return &PulsarBroker{
		Ctx:      ctx,
		Config:   nil,
		Consumer: consumer,
		Producer: producer,
	}, nil
}

func (p *PulsarBroker) CreateTopic(request CreateTopicRequest) CreateTopicResponse {
	panic("implement me")
}

func (p *PulsarBroker) DeleteTopic(request DeleteTopicRequest) DeleteTopicResponse {
	panic("implement me")
}

func (p *PulsarBroker) SendMessage(request SendMessageToTopicRequest) SendMessageToTopicResponse {
	msgId, err := p.Producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: request.Message,
	})

	return SendMessageToTopicResponse{
		BaseResponse: BaseResponse{
			Error: err,
		},
		MessageId: string(msgId.Serialize()),
	}
}

func (p *PulsarBroker) GetMessages(request GetMessagesFromTopicRequest) GetMessagesFromTopicResponse {
	channel := make(chan pulsar.ConsumerMessage, 100)

	msgs := make([]string, 0)
	for cm := range channel {
		msg := cm.Message
		msgs = append(msgs, string(msg.ID().Serialize()))
		p.Consumer.Ack(msg)
	}

	return GetMessagesFromTopicResponse{
		Messages: msgs,
	}
}

func (p *PulsarBroker) Commit() {
	panic("implement me")
}
