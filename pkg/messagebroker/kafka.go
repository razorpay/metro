package messagebroker

import (
	"context"
	"fmt"
	"strings"
	"time"

	kakfapkg "github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaBroker for kafka
type KafkaBroker struct {
	Producer *kakfapkg.Producer
	Consumer *kakfapkg.Consumer
	Admin    *kakfapkg.AdminClient
	Ctx      context.Context

	// holds the broker config
	Config *BrokerConfig

	// holds the client configs
	POptions *ProducerClientOptions
	COptions *ConsumerClientOptions
	AOptions *AdminClientOptions
}

// NewKafkaConsumerClient returns a kafka consumer
func NewKafkaConsumerClient(ctx context.Context, bConfig *BrokerConfig, options *ConsumerClientOptions) (Consumer, error) {
	err := validateKafkaConsumerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaConsumerClientConfig(options)
	if err != nil {
		return nil, err
	}

	c, err := kakfapkg.NewConsumer(&kakfapkg.ConfigMap{
		"bootstrap.servers":  bConfig.Brokers,
		"group.id":           options.GroupID,
		"auto.offset.reset":  bConfig.Consumer.OffsetReset,
		"enable.auto.commit": bConfig.Consumer.EnableAutoCommit,
	})

	if err != nil {
		return nil, err
	}

	c.SubscribeTopics([]string{options.Topic}, nil)

	return &KafkaBroker{
		Consumer: c,
		Ctx:      ctx,
		Config:   bConfig,
		COptions: options,
	}, nil
}

// NewKafkaProducerClient returns a kafka producer
func NewKafkaProducerClient(ctx context.Context, bConfig *BrokerConfig, options *ProducerClientOptions) (Producer, error) {
	err := validateKafkaProducerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaProducerClientConfig(options)
	if err != nil {
		return nil, err
	}

	p, err := kakfapkg.NewProducer(&kakfapkg.ConfigMap{"bootstrap.servers": strings.Join(bConfig.Brokers, ",")})
	if err != nil {
		return nil, err
	}

	return &KafkaBroker{
		Producer: p,
		Ctx:      ctx,
		Config:   bConfig,
		POptions: options,
	}, nil
}

// NewKafkaAdminClient returns a kafka admin
func NewKafkaAdminClient(ctx context.Context, bConfig *BrokerConfig, options *AdminClientOptions) (Admin, error) {
	err := validateKafkaAdminBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaAdminClientConfig(options)
	if err != nil {
		return nil, err
	}

	a, err := kakfapkg.NewAdminClient(&kakfapkg.ConfigMap{"bootstrap.servers": strings.Join(bConfig.Brokers, ",")})
	if err != nil {
		return nil, err
	}

	return &KafkaBroker{
		Admin:    a,
		Ctx:      ctx,
		Config:   bConfig,
		AOptions: options,
	}, nil
}

// CreateTopic creates a new topic if not available
func (k *KafkaBroker) CreateTopic(ctx context.Context, request CreateTopicRequest) (CreateTopicResponse, error) {

	topics := make([]kakfapkg.TopicSpecification, 0)
	ts := kakfapkg.TopicSpecification{
		Topic:         request.Name,
		NumPartitions: request.NumPartitions,
	}
	topics = append(topics, ts)
	resp, err := k.Admin.CreateTopics(k.Ctx, topics, nil)

	return CreateTopicResponse{
		Response: resp,
	}, err
}

// DeleteTopic deletes an existing topic
func (k *KafkaBroker) DeleteTopic(ctx context.Context, request DeleteTopicRequest) (DeleteTopicResponse, error) {

	topics := make([]string, 0)
	topics = append(topics, request.Name)
	resp, err := k.Admin.DeleteTopics(k.Ctx, topics)
	return DeleteTopicResponse{
		Response: resp,
	}, err
}

// SendMessages sends a message on the topic
func (k *KafkaBroker) SendMessages(ctx context.Context, request SendMessageToTopicRequest) (SendMessageToTopicResponse, error) {

	var kHeaders []kakfapkg.Header
	if request.Attributes != nil {
		for _, attribute := range request.Attributes {
			for k, v := range attribute {
				kHeaders = append(kHeaders, kakfapkg.Header{
					Key:   k,
					Value: v,
				})
			}
		}
	}

	deliveryChan := make(chan kakfapkg.Event)

	err := k.Producer.Produce(&kakfapkg.Message{
		TopicPartition: kakfapkg.TopicPartition{Topic: &request.Topic, Partition: kakfapkg.PartitionAny},
		Value:          request.Message,
		Headers:        kHeaders,
	}, deliveryChan)

	var m *kakfapkg.Message
	select {
	case err := <-deliveryChan:
		m = err.(*kakfapkg.Message)
	case <-time.After(time.Duration(k.POptions.Timeout)):
		return SendMessageToTopicResponse{}, fmt.Errorf("failed to produce message to topic [%v] due to timeout [%v]", &request.Topic, k.POptions.Timeout)
	}

	if m != nil && m.TopicPartition.Error != nil {
		return SendMessageToTopicResponse{
			MessageID: m.String(),
			Response:  err,
		}, m.TopicPartition.Error
	}

	close(deliveryChan)

	return SendMessageToTopicResponse{MessageID: m.String()}, err
}

//ReceiveMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
//from the previous committed offset. If the available messages in the queue are less, returns
// how many ever messages are available
func (k *KafkaBroker) ReceiveMessages(ctx context.Context, request GetMessagesFromTopicRequest) (GetMessagesFromTopicResponse, error) {

	var msgs []string
	for {
		var interval time.Duration
		interval = k.Config.Consumer.PollInterval
		if len(msgs) == 0 {
			interval = request.Timeout
		}

		msg, err := k.Consumer.ReadMessage(interval)
		if err == nil {
			msgs = append(msgs, string(msg.Value))
			if len(msgs) == request.NumOfMessages {
				return GetMessagesFromTopicResponse{Messages: msgs}, nil
			}

		} else {
			// The client will automatically try to recover from all errors.
			return GetMessagesFromTopicResponse{
				Response: err,
			}, err
		}
	}
	return GetMessagesFromTopicResponse{Messages: msgs}, nil
}

// Commit Commits messages if any
//This func will commit the message consumed
//by all the previous calls to GetMessages
func (k *KafkaBroker) Commit(ctx context.Context, request CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	tp := kakfapkg.TopicPartition{
		Topic:     &request.Topic,
		Partition: request.Partition,
		Offset:    kakfapkg.Offset(request.Offset),
	}

	tps := make([]kakfapkg.TopicPartition, 0)
	tps = append(tps, tp)

	resp, err := k.Consumer.CommitOffsets(tps)
	return CommitOnTopicResponse{
		Response: resp,
	}, err
}
