package messagebroker

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// PulsarBroker for pulsar
type PulsarBroker struct {
	Ctx      context.Context
	Config   *BrokerConfig
	Consumer pulsar.Consumer
	Producer pulsar.Producer
}

// NewPulsarConsumer returns a pulsar consumer
func NewPulsarConsumer(ctx context.Context, bConfig *BrokerConfig) (Consumer, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               bConfig.Brokers[0],
		OperationTimeout:  time.Duration(bConfig.OperationTimeout) * time.Second,
		ConnectionTimeout: time.Duration(bConfig.ConnectionTimeout) * time.Second,
	})

	if err != nil {
		return nil, err
	}

	c, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            bConfig.Consumer.Topic,
		SubscriptionName: bConfig.Consumer.Subscription,
		Type:             pulsar.SubscriptionType(bConfig.Consumer.SubscriptionType),
	})

	if err != nil {
		return nil, err
	}

	return &PulsarBroker{
		Ctx:      ctx,
		Config:   bConfig,
		Consumer: c,
	}, nil
}

// NewPulsarProducer returns a pulsar producer
func NewPulsarProducer(ctx context.Context, bConfig *BrokerConfig) (Producer, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               bConfig.Brokers[0],
		OperationTimeout:  time.Duration(bConfig.OperationTimeout) * time.Second,
		ConnectionTimeout: time.Duration(bConfig.ConnectionTimeout) * time.Second,
	})

	if err != nil {
		return nil, err
	}

	p, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: bConfig.Producer.Topic,
	})

	if err != nil {
		return nil, err
	}

	return &PulsarBroker{
		Ctx:      ctx,
		Config:   bConfig,
		Producer: p,
	}, nil
}

// NewPulsarAdmin returns a pulsar admin
func NewPulsarAdmin(ctx context.Context, bConfig *BrokerConfig) (Admin, error) {
	// admin client init
	// Need to write an Admin wrapper over http://pulsar.apache.org/docs/v2.0.1-incubating/reference/RestApi/
	// https://streamnative.io/en/blog/tech/2019-11-26-introduction-pulsarctl
	return &PulsarBroker{
		Ctx:    ctx,
		Config: bConfig,
	}, nil
}

// CreateTopic creates a new topic if not available
func (p PulsarBroker) CreateTopic(ctx context.Context, request CreateTopicRequest) (CreateTopicResponse, error) {
	panic("implement me")
}

// DeleteTopic deletes an existing topic
func (p PulsarBroker) DeleteTopic(ctx context.Context, request DeleteTopicRequest) (DeleteTopicResponse, error) {
	panic("implement me")
}

// SendMessage sends a message on the topic
func (p PulsarBroker) SendMessage(ctx context.Context, request SendMessageToTopicRequest) (SendMessageToTopicResponse, error) {
	msgID, err := p.Producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: request.Message,
	})

	return SendMessageToTopicResponse{
		MessageID: string(msgID.Serialize()),
	}, err
}

//GetMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
//from the previous committed offset. If the available messages in the queue are less, returns
// how many ever messages are available
func (p PulsarBroker) GetMessages(ctx context.Context, request GetMessagesFromTopicRequest) (GetMessagesFromTopicResponse, error) {

	channel := make(chan pulsar.ConsumerMessage, request.NumOfMessages)

	msgs := make([]string, 0)
	for cm := range channel {
		msg := cm.Message
		msgs = append(msgs, string(msg.ID().Serialize()))
	}

	return GetMessagesFromTopicResponse{
		Messages: msgs,
	}, nil
}

//Commit Commits messages if any
//This func will commit the message consumed
//by all the previous calls to GetMessages
func (p PulsarBroker) Commit(ctx context.Context) (CommitOnTopicResponse, error) {
	panic("implement me")
}
