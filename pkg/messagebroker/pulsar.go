package messagebroker

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// PulsarBroker for pulsar
type PulsarBroker struct {
	Ctx      context.Context
	Consumer pulsar.Consumer
	Producer pulsar.Producer

	// holds the broker config
	Config *BrokerConfig

	// holds the client configs
	POptions *ProducerClientOptions
	COptions *ConsumerClientOptions
	AOptions *AdminClientOptions
}

// newPulsarConsumerClient returns a pulsar consumer
func newPulsarConsumerClient(ctx context.Context, bConfig *BrokerConfig, options *ConsumerClientOptions) (Consumer, error) {

	err := validatePulsarConsumerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validatePulsarConsumerClientConfig(options)
	if err != nil {
		return nil, err
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               bConfig.Brokers[0],
		OperationTimeout:  time.Duration(bConfig.OperationTimeout) * time.Second,
		ConnectionTimeout: time.Duration(bConfig.ConnectionTimeout) * time.Second,
	})

	if err != nil {
		return nil, err
	}

	c, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            options.Topic,
		SubscriptionName: options.Subscription,
		Type:             pulsar.SubscriptionType(bConfig.Consumer.SubscriptionType),
	})

	if err != nil {
		return nil, err
	}

	return &PulsarBroker{
		Ctx:      ctx,
		Config:   bConfig,
		Consumer: c,
		COptions: options,
	}, nil
}

// newPulsarProducerClient returns a pulsar producer
func newPulsarProducerClient(ctx context.Context, bConfig *BrokerConfig, options *ProducerClientOptions) (Producer, error) {

	err := validatePulsarProducerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validatePulsarProducerClientConfig(options)
	if err != nil {
		return nil, err
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               bConfig.Brokers[0],
		OperationTimeout:  time.Duration(bConfig.OperationTimeout) * time.Second,
		ConnectionTimeout: time.Duration(bConfig.ConnectionTimeout) * time.Second,
	})

	if err != nil {
		return nil, err
	}

	p, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: options.Topic,
	})

	if err != nil {
		return nil, err
	}

	return &PulsarBroker{
		Ctx:      ctx,
		Config:   bConfig,
		Producer: p,
		POptions: options,
	}, nil
}

// newPulsarAdminClient returns a pulsar admin
func newPulsarAdminClient(ctx context.Context, bConfig *BrokerConfig, options *AdminClientOptions) (Admin, error) {
	err := validatePulsarAdminBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validatePulsarAdminClientConfig(options)
	if err != nil {
		return nil, err
	}

	// admin client init
	// Need to write an Admin wrapper over http://pulsar.apache.org/docs/v2.0.1-incubating/reference/RestApi/
	// https://streamnative.io/en/blog/tech/2019-11-26-introduction-pulsarctl
	return &PulsarBroker{
		Ctx:      ctx,
		Config:   bConfig,
		AOptions: options,
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

// SendMessages sends a message on the topic
func (p PulsarBroker) SendMessages(ctx context.Context, request SendMessageToTopicRequest) (SendMessageToTopicResponse, error) {
	msgID, err := p.Producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: request.Message,
	})

	return SendMessageToTopicResponse{
		MessageID: string(msgID.Serialize()),
	}, err
}

//ReceiveMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
//from the previous committed offset. If the available messages in the queue are less, returns
// how many ever messages are available
func (p PulsarBroker) ReceiveMessages(ctx context.Context, request GetMessagesFromTopicRequest) (GetMessagesFromTopicResponse, error) {

	msgs := make([]string, request.NumOfMessages)
	for i := 0; i < request.NumOfMessages; i++ {
		msg, err := p.Consumer.Receive(ctx)
		if err != nil {
			return GetMessagesFromTopicResponse{
				Response: err,
			}, err
		}
		msgs = append(msgs, string(msg.ID().Serialize()))
	}

	return GetMessagesFromTopicResponse{
		Messages: msgs,
	}, nil
}

//Commit Commits messages if any
//This func will commit the message consumed
//by all the previous calls to GetMessages
func (p PulsarBroker) Commit(ctx context.Context, request CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	panic("implement me")
}

// GetTopicMetadata ...
func (p PulsarBroker) GetTopicMetadata(ctx context.Context, request GetTopicMetadataRequest) (GetTopicMetadataResponse, error) {
	panic("implement me")
}
