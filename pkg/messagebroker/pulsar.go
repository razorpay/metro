package messagebroker

import (
	"context"
	"fmt"
	"time"

	"github.com/streamnative/pulsarctl/pkg/pulsar/common"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarctl "github.com/streamnative/pulsarctl/pkg/pulsar"
)

// PulsarBroker for pulsar
type PulsarBroker struct {
	Ctx      context.Context
	Consumer pulsar.Consumer
	Producer pulsar.Producer
	Admin    pulsarctl.Client

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
		OperationTimeout:  time.Duration(bConfig.OperationTimeoutSec) * time.Second,
		ConnectionTimeout: time.Duration(bConfig.ConnectionTimeoutSec) * time.Second,
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
		URL:                        fmt.Sprintf("pulsar://%v", bConfig.Brokers[0]),
		TLSAllowInsecureConnection: true,
		OperationTimeout:           time.Duration(bConfig.OperationTimeoutSec) * time.Second,
		ConnectionTimeout:          time.Duration(bConfig.ConnectionTimeoutSec) * time.Second,
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

	admin, err := pulsarctl.New(&common.Config{
		WebServiceURL:              fmt.Sprintf("http://%v", bConfig.Brokers[0]),
		TLSAllowInsecureConnection: true,
	})

	if err != nil {
		return nil, err
	}

	return &PulsarBroker{
		Ctx:      ctx,
		Config:   bConfig,
		AOptions: options,
		Admin:    admin,
	}, nil
}

// CreateTopic creates a new topic if not available
func (p *PulsarBroker) CreateTopic(ctx context.Context, request CreateTopicRequest) (CreateTopicResponse, error) {

	pulsarTopic, terr := utils.GetTopicName(request.Name)
	if terr != nil {
		return CreateTopicResponse{}, terr
	}

	err := p.Admin.Topics().Create(*pulsarTopic, request.NumPartitions)
	if err != nil {
		return CreateTopicResponse{}, err
	}

	return CreateTopicResponse{}, nil
}

// DeleteTopic deletes an existing topic
func (p *PulsarBroker) DeleteTopic(ctx context.Context, request DeleteTopicRequest) (DeleteTopicResponse, error) {

	pulsarTopic, terr := utils.GetTopicName(request.Name)
	if terr != nil {
		return DeleteTopicResponse{}, terr
	}

	err := p.Admin.Topics().Delete(*pulsarTopic, request.Force, request.NonPartioned)
	if err != nil {
		return DeleteTopicResponse{}, err
	}

	return DeleteTopicResponse{}, nil
}

// SendMessages sends a message on the topic
func (p *PulsarBroker) SendMessages(ctx context.Context, request SendMessageToTopicRequest) (SendMessageToTopicResponse, error) {
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
func (p *PulsarBroker) ReceiveMessages(ctx context.Context, request GetMessagesFromTopicRequest) (GetMessagesFromTopicResponse, error) {

	msgs := make(map[string]string, request.NumOfMessages)
	for i := 0; i < request.NumOfMessages; i++ {
		msg, err := p.Consumer.Receive(ctx)
		if err != nil {
			return GetMessagesFromTopicResponse{
				Response: err,
			}, err
		}
		msgs[string(msg.ID().Serialize())] = string(msg.Payload())
	}

	return GetMessagesFromTopicResponse{
		OffsetWithMessages: msgs,
	}, nil
}

//Commit Commits messages if any
//This func will commit the message consumed
//by all the previous calls to GetMessages
func (p *PulsarBroker) Commit(ctx context.Context, request CommitOnTopicRequest) (CommitOnTopicResponse, error) {

	p.Consumer.AckID(&pulsarAckMessage{
		ID: request.ID,
	})

	return CommitOnTopicResponse{}, nil
}

// GetTopicMetadata ...
func (p *PulsarBroker) GetTopicMetadata(ctx context.Context, request GetTopicMetadataRequest) (GetTopicMetadataResponse, error) {
	pulsarTopic, terr := utils.GetTopicName(request.Topic)
	if terr != nil {
		return GetTopicMetadataResponse{}, terr
	}

	stats, err := p.Admin.Topics().GetInternalStats(*pulsarTopic)
	if err != nil {
		return GetTopicMetadataResponse{}, err
	}

	return GetTopicMetadataResponse{
		Response: stats,
	}, nil
}
