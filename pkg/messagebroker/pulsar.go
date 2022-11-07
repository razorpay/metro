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
func newPulsarConsumerClient(_ context.Context, bConfig *BrokerConfig, options *ConsumerClientOptions) (Consumer, error) {

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
		OperationTimeout:  time.Duration(bConfig.OperationTimeoutMs) * time.Millisecond,
		ConnectionTimeout: time.Duration(bConfig.ConnectionTimeoutMs) * time.Millisecond,
	})

	if err != nil {
		return nil, err
	}

	c, err := client.Subscribe(pulsar.ConsumerOptions{
		Topics:           options.Topics,
		SubscriptionName: options.Subscription,
		Type:             pulsar.SubscriptionType(bConfig.Consumer.SubscriptionType),
		Name:             options.GroupInstanceID,
	})

	if err != nil {
		return nil, err
	}

	return &PulsarBroker{
		Config:   bConfig,
		Consumer: c,
		COptions: options,
	}, nil
}

// newPulsarProducerClient returns a pulsar producer
func newPulsarProducerClient(_ context.Context, bConfig *BrokerConfig, options *ProducerClientOptions) (Producer, error) {

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
		OperationTimeout:           time.Duration(bConfig.OperationTimeoutMs) * time.Millisecond,
		ConnectionTimeout:          time.Duration(bConfig.ConnectionTimeoutMs) * time.Millisecond,
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
		Config:   bConfig,
		AOptions: options,
		Admin:    admin,
	}, nil
}

// FetchConsumerLag ...
func (p *PulsarBroker) FetchConsumerLag(ctx context.Context) (map[string]uint64, error) {
	lag := make(map[string]uint64)

	return lag, nil
}

// CreateTopic creates a new topic if not available
func (p *PulsarBroker) CreateTopic(ctx context.Context, request CreateTopicRequest) (CreateTopicResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Pulsar, "CreateTopic").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Pulsar, "CreateTopic").Observe(time.Now().Sub(startTime).Seconds())
	}()

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
	messageBrokerOperationCount.WithLabelValues(env, Pulsar, "DeleteTopic").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Pulsar, "DeleteTopic").Observe(time.Now().Sub(startTime).Seconds())
	}()

	pulsarTopic, terr := utils.GetTopicName(request.Name)
	if terr != nil {
		return DeleteTopicResponse{}, terr
	}

	err := p.Admin.Topics().Delete(*pulsarTopic, request.Force, request.NonPartitioned)
	if err != nil {
		return DeleteTopicResponse{}, err
	}

	return DeleteTopicResponse{}, nil
}

// SendMessage sends a message on the topic
func (p PulsarBroker) SendMessage(ctx context.Context, request SendMessageToTopicRequest) (*SendMessageToTopicResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Pulsar, "SendMessage").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Pulsar, "SendMessage").Observe(time.Now().Sub(startTime).Seconds())
	}()

	msgID, err := p.Producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: request.Message,
	})

	return &SendMessageToTopicResponse{
		MessageID: string(msgID.Serialize()),
	}, err
}

// AlterTopicConfigs alters the topic config
func (p *PulsarBroker) AlterTopicConfigs(ctx context.Context, request ModifyTopicConfigRequest) ([]string, error) {
	panic("implement this!")
}

// DescribeTopicConfigs describes the topic config
func (p *PulsarBroker) DescribeTopicConfigs(ctx context.Context, names []string) (map[string]map[string]string, error) {
	panic("implement this!")
}

// ReceiveMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
// from the previous committed offset. If the available messages in the queue are less, returns
// how many ever messages are available
func (p PulsarBroker) ReceiveMessages(ctx context.Context, request GetMessagesFromTopicRequest) (*GetMessagesFromTopicResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Pulsar, "ReceiveMessages").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Pulsar, "ReceiveMessages").Observe(time.Now().Sub(startTime).Seconds())
	}()

	var i int32
	msgs := make([]ReceivedMessage, 0)
	for i = 0; i < request.NumOfMessages; i++ {
		msg, err := p.Consumer.Receive(ctx)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, ReceivedMessage{
			Data: msg.Payload(),
			MessageHeader: MessageHeader{
				MessageID:   string(msg.ID().Serialize()),
				PublishTime: msg.PublishTime(),
			},
		})
	}

	return &GetMessagesFromTopicResponse{
		Messages: msgs,
	}, nil
}

// CommitByPartitionAndOffset Commits messages if any
// This func will commit the message consumed
// by all the previous calls to GetMessages
func (p *PulsarBroker) CommitByPartitionAndOffset(_ context.Context, _ CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	// unused for pulsar
	return CommitOnTopicResponse{}, nil
}

// CommitByMsgID Commits a message by ID
func (p *PulsarBroker) CommitByMsgID(ctx context.Context, request CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Pulsar, "CommitByMsgID").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Pulsar, "CommitByMsgID").Observe(time.Now().Sub(startTime).Seconds())
	}()

	p.Consumer.AckID(&pulsarAckMessage{
		ID: request.ID,
	})

	return CommitOnTopicResponse{}, nil
}

// GetTopicMetadata ...
func (p *PulsarBroker) GetTopicMetadata(ctx context.Context, request GetTopicMetadataRequest) (GetTopicMetadataResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Pulsar, "GetTopicMetadata").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Pulsar, "GetTopicMetadata").Observe(time.Now().Sub(startTime).Seconds())
	}()

	pulsarTopic, terr := utils.GetTopicName(request.Topic)
	if terr != nil {
		return GetTopicMetadataResponse{}, terr
	}

	stats, err := p.Admin.Topics().GetInternalStats(*pulsarTopic)
	if err != nil {
		return GetTopicMetadataResponse{}, err
	}

	return GetTopicMetadataResponse{
		Topic:  request.Topic,
		Offset: int32(stats.Cursors[request.Topic].MessagesConsumedCounter),
	}, nil
}

// Pause pause the consumer
func (p *PulsarBroker) Pause(_ context.Context, _ PauseOnTopicRequest) error {
	// unused for pulsar
	return nil
}

// Resume resume the consumer
func (p *PulsarBroker) Resume(_ context.Context, _ ResumeOnTopicRequest) error {
	// unused for pulsar
	return nil
}

// Close closes the consumer
func (p *PulsarBroker) Close(_ context.Context) error {
	messageBrokerOperationCount.WithLabelValues(env, Pulsar, "Close").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Pulsar, "Close").Observe(time.Now().Sub(startTime).Seconds())
	}()

	p.Consumer.Close()
	return nil
}

// AddTopicPartitions adds partitions to an existing topic
func (p *PulsarBroker) AddTopicPartitions(_ context.Context, _ AddTopicPartitionRequest) (*AddTopicPartitionResponse, error) {
	// unused for pulsar
	return nil, nil
}

// FetchProjectTopics fetches a list of all topics for a given project
func (p *PulsarBroker) FetchProjectTopics(ctx context.Context, project string) (map[string]bool, error) {
	// unused for pulsar
	return nil, nil
}

// IsHealthy checks the health of pulsar
func (p *PulsarBroker) IsHealthy(_ context.Context) (bool, error) {
	err := p.Admin.Brokers().HealthCheck()
	return err == nil, err
}

// Shutdown closes the producer
func (p *PulsarBroker) Shutdown(ctx context.Context) {
	panic("implement this!")
}

// IsClosed checks if producer has been closed
func (p *PulsarBroker) IsClosed(_ context.Context) bool {
	panic("implement this!")
}

// Flush flushes the producer buffer
func (p *PulsarBroker) Flush(timeoutMs int) error {
	panic("implement this!")
}
