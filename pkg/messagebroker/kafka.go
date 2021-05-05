package messagebroker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/rs/xid"
)

const (
	messageID  = "messageID"
	retryCount = "retryCount"
)

// KafkaBroker for kafka
type KafkaBroker struct {
	Producer *kafkapkg.Producer
	Consumer *kafkapkg.Consumer
	Admin    *kafkapkg.AdminClient
	Ctx      context.Context

	// holds the broker config
	Config *BrokerConfig

	// holds the client configs
	POptions *ProducerClientOptions
	COptions *ConsumerClientOptions
	AOptions *AdminClientOptions
}

// newKafkaConsumerClient returns a kafka consumer
func newKafkaConsumerClient(ctx context.Context, bConfig *BrokerConfig, id string, options *ConsumerClientOptions) (Consumer, error) {
	err := validateKafkaConsumerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaConsumerClientConfig(options)
	if err != nil {
		return nil, err
	}

	configMap := &kafkapkg.ConfigMap{
		"bootstrap.servers":  strings.Join(bConfig.Brokers, ","),
		"group.id":           options.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"group.instance.id":  id,
	}

	logger.Ctx(ctx).Infow("kafka consumer: initializing new", "configMap", configMap, "options", options, "id", id)

	if bConfig.EnableTLS {
		certs, err := readKafkaCerts(bConfig.CertDir)
		if err != nil {
			return nil, err
		}

		// Refer : https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka#configure-librdkafka-client
		configMap.SetKey("security.protocol", "ssl")
		configMap.SetKey("ssl.ca.location", certs.caCertPath)
		configMap.SetKey("ssl.certificate.location", certs.userCertPath)
		configMap.SetKey("ssl.key.location", certs.userKeyPath)
	}

	c, err := kafkapkg.NewConsumer(configMap)
	if err != nil {
		return nil, err
	}

	c.SubscribeTopics(options.Topics, nil)

	logger.Ctx(ctx).Infow("kafka consumer: initialized")

	return &KafkaBroker{
		Consumer: c,
		Config:   bConfig,
		COptions: options,
	}, nil
}

// newKafkaProducerClient returns a kafka producer
func newKafkaProducerClient(ctx context.Context, bConfig *BrokerConfig, options *ProducerClientOptions) (Producer, error) {
	err := validateKafkaProducerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaProducerClientConfig(options)
	if err != nil {
		return nil, err
	}

	logger.Ctx(ctx).Infow("kafka producer: initializing new", "options", options)

	configMap := &kafkapkg.ConfigMap{
		"bootstrap.servers": strings.Join(bConfig.Brokers, ","),
	}

	if bConfig.EnableTLS {
		certs, err := readKafkaCerts(bConfig.CertDir)
		if err != nil {
			return nil, err
		}

		configMap.SetKey("security.protocol", "ssl")
		configMap.SetKey("ssl.ca.location", certs.caCertPath)
		configMap.SetKey("ssl.certificate.location", certs.userCertPath)
		configMap.SetKey("ssl.key.location", certs.userKeyPath)
	}

	p, err := kafkapkg.NewProducer(configMap)
	if err != nil {
		return nil, err
	}

	logger.Ctx(ctx).Infow("kafka producer: initialized")

	return &KafkaBroker{
		Producer: p,
		Config:   bConfig,
		POptions: options,
	}, nil
}

// newKafkaAdminClient returns a kafka admin
func newKafkaAdminClient(ctx context.Context, bConfig *BrokerConfig, options *AdminClientOptions) (Admin, error) {
	err := validateKafkaAdminBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaAdminClientConfig(options)
	if err != nil {
		return nil, err
	}

	logger.Ctx(ctx).Infow("kafka admin: initializing new", "options", *options)

	configMap := &kafkapkg.ConfigMap{
		"bootstrap.servers": strings.Join(bConfig.Brokers, ","),
	}

	if bConfig.EnableTLS {
		certs, err := readKafkaCerts(bConfig.CertDir)
		if err != nil {
			return nil, err
		}

		configMap.SetKey("security.protocol", "ssl")
		configMap.SetKey("ssl.ca.location", certs.caCertPath)
		configMap.SetKey("ssl.certificate.location", certs.userCertPath)
		configMap.SetKey("ssl.key.location", certs.userKeyPath)
	}

	a, err := kafkapkg.NewAdminClient(configMap)

	if err != nil {
		return nil, err
	}

	logger.Ctx(ctx).Infow("kafka admin: initialized")

	return &KafkaBroker{
		Admin:    a,
		Config:   bConfig,
		AOptions: options,
	}, nil
}

type kafkaCerts struct {
	caCertPath   string
	userCertPath string
	userKeyPath  string
}

func readKafkaCerts(certDir string) (*kafkaCerts, error) {
	caCertPath, err := getCertFile(certDir, "ca-cert.pem")
	if err != nil {
		return nil, err
	}
	userCertPath, err := getCertFile(certDir, "user-cert.pem")
	if err != nil {
		return nil, err
	}
	userKeyPath, err := getCertFile(certDir, "user.key")
	if err != nil {
		return nil, err
	}

	return &kafkaCerts{
		caCertPath:   caCertPath,
		userCertPath: userCertPath,
		userKeyPath:  userKeyPath,
	}, nil
}

// CreateTopic creates a new topic if not available
func (k *KafkaBroker) CreateTopic(ctx context.Context, request CreateTopicRequest) (CreateTopicResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "CreateTopic").Inc()

	startTime := time.Now()
	defer messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "CreateTopic").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	tp := NormalizeTopicName(request.Name)
	logger.Ctx(ctx).Infow("received request to create kafka topic", "request", request, "normalizedTopicName", tp)

	topics := make([]kafkapkg.TopicSpecification, 0)
	ts := kafkapkg.TopicSpecification{
		Topic:             tp,
		NumPartitions:     request.NumPartitions,
		ReplicationFactor: 1,
	}
	topics = append(topics, ts)
	topicsResp, err := k.Admin.CreateTopics(ctx, topics, kafkapkg.SetAdminOperationTimeout(59*time.Second))
	if err != nil {
		messageBrokerOperationError.WithLabelValues(env, Kafka, "CreateTopic", err.Error()).Inc()
		return CreateTopicResponse{
			Response: topicsResp,
		}, err
	}

	for _, tp := range topicsResp {
		if tp.Error.Code() != kafkapkg.ErrNoError && tp.Error.Code() != kafkapkg.ErrTopicAlreadyExists {
			messageBrokerOperationError.WithLabelValues(env, Kafka, "CreateTopic", err.Error()).Inc()
			return CreateTopicResponse{
				Response: topicsResp,
			}, fmt.Errorf("kafka: %v", tp.Error.String())
		}
	}

	logger.Ctx(ctx).Infow("kafka topic creation successfully completed", "response", topicsResp)

	return CreateTopicResponse{
		Response: topicsResp,
	}, nil
}

// DeleteTopic deletes an existing topic
func (k *KafkaBroker) DeleteTopic(ctx context.Context, request DeleteTopicRequest) (DeleteTopicResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "DeleteTopic").Inc()

	startTime := time.Now()
	defer messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "DeleteTopic").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	topics := make([]string, 0)
	topics = append(topics, request.Name)
	resp, err := k.Admin.DeleteTopics(ctx, topics)
	if err != nil {
		messageBrokerOperationError.WithLabelValues(env, Kafka, "DeleteTopic", err.Error()).Inc()
		return DeleteTopicResponse{
			Response: resp,
		}, err
	}

	return DeleteTopicResponse{
		Response: resp,
	}, nil
}

// GetTopicMetadata fetches the given topics metadata stored in the broker
func (k *KafkaBroker) GetTopicMetadata(_ context.Context, request GetTopicMetadataRequest) (GetTopicMetadataResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "GetTopicMetadata").Inc()

	startTime := time.Now()
	defer messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "GetTopicMetadata").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	tp := kafkapkg.TopicPartition{
		Topic:     &request.Topic,
		Partition: request.Partition,
	}

	tps := make([]kafkapkg.TopicPartition, 0)
	tps = append(tps, tp)

	// TODO : normalize timeouts
	resp, err := k.Consumer.Committed(tps, 5000)
	if err != nil || resp == nil || len(resp) == 0 {
		messageBrokerOperationError.WithLabelValues(env, Kafka, "GetTopicMetadata", err.Error()).Inc()
		return GetTopicMetadataResponse{}, err
	}

	tpStats := resp[0]
	offset, _ := strconv.ParseInt(tpStats.Offset.String(), 10, 0)
	return GetTopicMetadataResponse{
		Topic:     request.Topic,
		Partition: request.Partition,
		Offset:    int32(offset),
	}, err
}

// SendMessage sends a message on the topic
func (k *KafkaBroker) SendMessage(ctx context.Context, request SendMessageToTopicRequest) (*SendMessageToTopicResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "SendMessage").Inc()

	startTime := time.Now()
	defer messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "SendMessage").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	var kHeaders []kafkapkg.Header
	if request.Attributes != nil {
		for _, attribute := range request.Attributes {
			for k, v := range attribute {
				kHeaders = append(kHeaders, kafkapkg.Header{
					Key:   k,
					Value: v,
				})
			}
		}
	}

	msgID := request.MessageID
	if msgID == "" {
		// generate a message id and attach only if not sent by the caller
		// in case of retry push to topic, the same messageID is to be re-used
		msgID = xid.New().String()
	}

	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   messageID,
		Value: []byte(msgID),
	})

	rc, _ := json.Marshal(request.RetryCount)
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   retryCount,
		Value: rc,
	})

	deliveryChan := make(chan kafkapkg.Event, 1000)
	defer close(deliveryChan)

	tp := NormalizeTopicName(request.Topic)
	logger.Ctx(ctx).Infow("normalized topic name", "topic", tp)
	err := k.Producer.Produce(&kafkapkg.Message{
		TopicPartition: kafkapkg.TopicPartition{Topic: &tp, Partition: kafkapkg.PartitionAny},
		Value:          request.Message,
		Key:            []byte(request.OrderingKey),
		Headers:        kHeaders,
	}, deliveryChan)
	if err != nil {
		messageBrokerOperationError.WithLabelValues(env, Kafka, "SendMessage", err.Error()).Inc()
		return nil, err
	}

	// if timeout not send, override with default timeout set during client creation
	timeout := request.TimeoutMs
	if timeout == 0 {
		timeout = int(k.POptions.TimeoutMs)
	}

	var m *kafkapkg.Message
	select {
	case event := <-deliveryChan:
		m = event.(*kafkapkg.Message)
		//case <-time.After(time.Duration(request.TimeoutMs) * time.Millisecond):
		//	return nil, fmt.Errorf("failed to produce message to topic [%v] due to timeout [%v]", request.Topic, request.TimeoutMs)
	}

	if m != nil && m.TopicPartition.Error != nil {
		logger.Ctx(ctx).Errorw("kafka: error in publishing messages", "error", m.TopicPartition.Error.Error())
		messageBrokerOperationError.WithLabelValues(env, Kafka, "SendMessage", err.Error()).Inc()
		return nil, m.TopicPartition.Error
	}

	return &SendMessageToTopicResponse{MessageID: msgID}, nil
}

//ReceiveMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
//from the previous committed offset. If the available messages in the queue are less, returns
// how many ever messages are available
func (k *KafkaBroker) ReceiveMessages(ctx context.Context, request GetMessagesFromTopicRequest) (*GetMessagesFromTopicResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "ReceiveMessages").Inc()

	startTime := time.Now()
	defer messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "ReceiveMessages").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	msgs := make(map[string]ReceivedMessage, 0)
	for {
		var msgID string
		var retryCounter int32
		msg, err := k.Consumer.ReadMessage(time.Duration(request.TimeoutMs) * time.Millisecond)
		if err == nil {
			for _, v := range msg.Headers {
				if v.Key == messageID {
					msgID = string(v.Value)
				} else if v.Key == retryCount {
					json.Unmarshal(v.Value, &retryCounter)
				}
			}

			offset, _ := strconv.ParseInt(fmt.Sprintf("%v", int32(msg.TopicPartition.Offset)), 10, 0)
			po := NewPartitionOffset(msg.TopicPartition.Partition, int32(offset))
			msgs[po.String()] = ReceivedMessage{
				Data: msg.Value, MessageID: msgID,
				Topic:       *msg.TopicPartition.Topic,
				Partition:   msg.TopicPartition.Partition,
				Offset:      int32(msg.TopicPartition.Offset),
				RetryCount:  retryCounter,
				PublishTime: msg.Timestamp,
			}

			if int32(len(msgs)) == request.NumOfMessages {
				return &GetMessagesFromTopicResponse{PartitionOffsetWithMessages: msgs}, nil
			}
		} else if err.(kafkapkg.Error).Code() == kafkapkg.ErrTimedOut {
			messageBrokerOperationError.WithLabelValues(env, Kafka, "ReceiveMessages", err.Error()).Inc()
			return &GetMessagesFromTopicResponse{PartitionOffsetWithMessages: msgs}, nil
		} else {
			// The client will automatically try to recover from all errors.
			logger.Ctx(ctx).Errorw("kafka: error in receiving messages", "msg", err.Error())
			messageBrokerOperationError.WithLabelValues(env, Kafka, "ReceiveMessages", err.Error()).Inc()
			return nil, err
		}
	}

	return &GetMessagesFromTopicResponse{PartitionOffsetWithMessages: msgs}, nil
}

// CommitByPartitionAndOffset Commits messages if any
//This func will commit the message consumed
//by all the previous calls to GetMessages
func (k *KafkaBroker) CommitByPartitionAndOffset(ctx context.Context, request CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "CommitByPartitionAndOffset").Inc()

	startTime := time.Now()
	defer messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "CommitByPartitionAndOffset").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	logger.Ctx(ctx).Infow("kafka: commit request received", "request", request)

	tp := kafkapkg.TopicPartition{
		Topic:     &request.Topic,
		Partition: request.Partition,
		Offset:    kafkapkg.Offset(request.Offset),
	}

	tps := make([]kafkapkg.TopicPartition, 0)
	tps = append(tps, tp)

	attempt := 1
	resp, err := k.Consumer.CommitOffsets(tps)
	for {
		if err != nil && err.Error() == kafkapkg.ErrRequestTimedOut.String() && attempt <= 3 {
			logger.Ctx(ctx).Infow("kafka: retrying commit", "attempt", attempt)
			messageBrokerOperationError.WithLabelValues(env, Kafka, "CommitByPartitionAndOffset", err.Error()).Inc()
			resp, err = k.Consumer.CommitOffsets(tps)
			attempt++
			time.Sleep(time.Millisecond * 250)
			continue
		}
		break
	}

	if err != nil {
		logger.Ctx(ctx).Errorw("kafka: commit failed", "request", request, "error", err.Error())
		messageBrokerOperationError.WithLabelValues(env, Kafka, "CommitByPartitionAndOffset", err.Error()).Inc()
		return CommitOnTopicResponse{
			Response: nil,
		}, err
	}

	logger.Ctx(ctx).Infow("kafka: committed successfully", "request", request)

	return CommitOnTopicResponse{
		Response: resp,
	}, nil
}

// CommitByMsgID Commits a message by ID
func (k *KafkaBroker) CommitByMsgID(_ context.Context, _ CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	// unused for kafka
	return CommitOnTopicResponse{}, nil
}

// Pause pause the consumer
func (k *KafkaBroker) Pause(_ context.Context, request PauseOnTopicRequest) error {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "Pause").Inc()

	startTime := time.Now()
	defer messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "Pause").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	tp := kafkapkg.TopicPartition{
		Topic:     &request.Topic,
		Partition: request.Partition,
	}

	tps := make([]kafkapkg.TopicPartition, 0)
	tps = append(tps, tp)

	return k.Consumer.Pause(tps)
}

// Resume resume the consumer
func (k *KafkaBroker) Resume(_ context.Context, request ResumeOnTopicRequest) error {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "Resume").Inc()

	startTime := time.Now()
	defer messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "Resume").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	tp := kafkapkg.TopicPartition{
		Topic:     &request.Topic,
		Partition: request.Partition,
	}

	tps := make([]kafkapkg.TopicPartition, 0)
	tps = append(tps, tp)

	return k.Consumer.Resume(tps)
}

// Close closes the consumer
func (k *KafkaBroker) Close(ctx context.Context) error {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "Close").Inc()

	startTime := time.Now()
	defer messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "Close").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	logger.Ctx(ctx).Infow("kafka: request to close the consumer", "topic", k.COptions.Topics, "groupID", k.COptions.GroupID)
	err := k.Consumer.Unsubscribe()
	if err != nil {
		logger.Ctx(ctx).Errorw("kafka: consumer unsubscribe failed", "topic", k.COptions.Topics, "groupID", k.COptions.GroupID, "error", err.Error())
		messageBrokerOperationError.WithLabelValues(env, Kafka, "Close", err.Error()).Inc()
		return err
	}

	cerr := k.Consumer.Close()
	if cerr != nil {
		logger.Ctx(ctx).Errorw("kafka: consumer close failed", "topic", k.COptions.Topics, "groupID", k.COptions.GroupID, "error", cerr.Error())
		messageBrokerOperationError.WithLabelValues(env, Kafka, "Close", err.Error()).Inc()
		return cerr
	}
	logger.Ctx(ctx).Infow("kafka: consumer closed...", "topic", k.COptions.Topics, "groupID", k.COptions.GroupID)

	return nil
}
