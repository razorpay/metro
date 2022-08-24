package messagebroker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/razorpay/metro/pkg/logger"
)

var (
	errProducerUnavailable = errors.New("producer unavailable")
	kafkaMetadataTimeout   = 1000
)

// KafkaBroker for kafka
type KafkaBroker struct {
	Producer *kafkapkg.Producer
	Consumer *kafkapkg.Consumer
	Admin    *kafkapkg.AdminClient

	// holds the broker config
	Config *BrokerConfig

	// holds the client configs
	POptions *ProducerClientOptions
	COptions *ConsumerClientOptions
	AOptions *AdminClientOptions

	// flags
	isProducerClosed bool
}

// newKafkaConsumerClient returns a kafka consumer
func newKafkaConsumerClient(ctx context.Context, bConfig *BrokerConfig, options *ConsumerClientOptions) (Consumer, error) {

	normalizedTopics := make([]string, 0)
	for _, topic := range options.Topics {
		normalizedTopics = append(normalizedTopics, normalizeTopicName(topic))
	}

	err := validateKafkaConsumerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaConsumerClientConfig(options)
	if err != nil {
		return nil, err
	}

	if options.AutoOffsetReset == "" {
		options.AutoOffsetReset = "latest"
	}

	configMap := &kafkapkg.ConfigMap{
		"bootstrap.servers":       strings.Join(bConfig.Brokers, ","),
		"socket.keepalive.enable": true,
		"auto.offset.reset":       options.AutoOffsetReset,
		"enable.auto.commit":      false,
		"group.id":                options.GroupID,
		"group.instance.id":       options.GroupInstanceID,
	}

	logger.Ctx(ctx).Infow("kafka consumer: initializing new", "configMap", configMap, "options", options)

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

	c.SubscribeTopics(normalizedTopics, nil)

	logger.Ctx(ctx).Infow("kafka consumer: initialized", "options", options)

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
		"bootstrap.servers":          strings.Join(bConfig.Brokers, ","),
		"socket.keepalive.enable":    true,
		"retries":                    3,
		"request.timeout.ms":         3000,
		"delivery.timeout.ms":        2000,
		"connections.max.idle.ms":    180000,
		"batch.num.messages":         1,
		"queue.buffering.max.kbytes": 4096, // Total message size sum allocated in buffer. Shared across topics/partitions
		"queue.buffering.max.ms":     100,
		"go.logs.channel.enable":     false, // Disable logs via channel
		"go.produce.channel.size":    100,   // Allocated buffer size for the produce channel.
		"go.delivery.reports":        true,  // Returns delivery acks
		"go.batch.producer":          true,  // Disable batch producer since it clubs calls to librdkafka across topics. This causes memory bloat.
		"debug":                      "broker,msg",
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

	go func() {
		logger.Ctx(ctx).Infow("starting producer log reader...", "topic", options.Topic)
		select {
		case <-ctx.Done():
			return
		case log := <-p.Logs():
			logger.Ctx(ctx).Infow("kafka producer logs", "topic", options.Topic, "log", log.String())
		}
	}()

	logger.Ctx(ctx).Infow("kafka producer: initialized", "options", options)

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
	span, ctx := opentracing.StartSpanFromContext(ctx, "Kafka.CreateTopic")
	defer span.Finish()

	messageBrokerOperationCount.WithLabelValues(env, Kafka, "CreateTopic").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "CreateTopic").Observe(time.Now().Sub(startTime).Seconds())
	}()

	tp := normalizeTopicName(request.Name)
	logger.Ctx(ctx).Infow("received request to create kafka topic", "request", request, "normalizedTopicName", tp)

	topics := make([]kafkapkg.TopicSpecification, 0)
	ts := kafkapkg.TopicSpecification{
		Topic:             tp,
		NumPartitions:     request.NumPartitions,
		ReplicationFactor: (len(k.Config.Brokers) + 1) / 2, // 50% of the available brokers
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
			messageBrokerOperationError.WithLabelValues(env, Kafka, "CreateTopic", tp.Error.String()).Inc()
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
	span, ctx := opentracing.StartSpanFromContext(ctx, "Kafka.DeleteTopic")
	defer span.Finish()

	messageBrokerOperationCount.WithLabelValues(env, Kafka, "DeleteTopic").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "DeleteTopic").Observe(time.Now().Sub(startTime).Seconds())
	}()

	topics := make([]string, 0)
	topicN := normalizeTopicName(request.Name)
	topics = append(topics, topicN)
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
func (k *KafkaBroker) GetTopicMetadata(ctx context.Context, request GetTopicMetadataRequest) (GetTopicMetadataResponse, error) {
	logger.Ctx(ctx).Infow("kafka: get metadata request received", "request", request)
	defer func() {
		logger.Ctx(ctx).Infow("kafka: get metadata request completed", "request", request)
	}()

	span, ctx := opentracing.StartSpanFromContext(ctx, "Kafka.GetMetadata")
	defer span.Finish()

	messageBrokerOperationCount.WithLabelValues(env, Kafka, "GetTopicMetadata").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "GetTopicMetadata").Observe(time.Now().Sub(startTime).Seconds())
	}()

	topicN := normalizeTopicName(request.Topic)
	tp := kafkapkg.TopicPartition{
		Topic:     &topicN,
		Partition: request.Partition,
	}

	tps := make([]kafkapkg.TopicPartition, 0)
	tps = append(tps, tp)

	// TODO : normalize timeouts
	resp, err := k.Consumer.Committed(tps, 5000)
	if err != nil {
		messageBrokerOperationError.WithLabelValues(env, Kafka, "GetTopicMetadata", err.Error()).Inc()
		return GetTopicMetadataResponse{}, err
	}

	tpStats := resp[0]
	offset, _ := strconv.ParseInt(tpStats.Offset.String(), 10, 0)
	return GetTopicMetadataResponse{
		Topic:     request.Topic,
		Partition: request.Partition,
		Offset:    int32(offset),
	}, nil
}

// SendMessage sends a message on the topic
func (k *KafkaBroker) SendMessage(ctx context.Context, request SendMessageToTopicRequest) (*SendMessageToTopicResponse, error) {

	messageBrokerOperationCount.WithLabelValues(env, Kafka, "SendMessage").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "SendMessage").Observe(time.Now().Sub(startTime).Seconds())
	}()

	logger.Ctx(ctx).Infow("kafka: send message appending headers to request", "topic", request.Topic)

	// populate the request with the proper messageID
	request.MessageID = getMessageID(request.MessageID)
	// generate the needed headers to be sent on the broker
	kHeaders := convertRequestToKafkaHeaders(request)

	spanCtx := GetSpanContext(ctx, flattenMapSlice(request.Attributes))
	var span opentracing.Span
	if spanCtx == nil {
		span, ctx = opentracing.StartSpanFromContext(
			ctx,
			"Kafka.SendMessage",
			opentracing.Tags{
				"topic":      request.Topic,
				"message_id": request.MessageID,
			},
		)
	} else {
		span, ctx = opentracing.StartSpanFromContext(
			ctx,
			"Kafka.SendMessage",
			SpanContextOption(spanCtx),
			opentracing.Tags{
				"topic":      request.Topic,
				"message_id": request.MessageID,
			},
		)
	}
	defer span.Finish()

	logger.Ctx(ctx).Infow("kafka: send message appending headers to request completed", "request", request.Topic, "kHeaders", kHeaders)

	// Adds the span context in the headers of message
	// This header data will be used by consumer to resume the current context
	carrier := kafkaHeadersCarrier(kHeaders)
	injectErr := opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, &carrier)
	if injectErr != nil {
		logger.Ctx(ctx).Warnw("error injecting span context in message headers", "error", injectErr.Error())
	}

	deliveryChan := make(chan kafkapkg.Event, 1)
	defer close(deliveryChan)

	topicN := normalizeTopicName(request.Topic)
	logger.Ctx(ctx).Debugw("normalized topic name", "topic", topicN)

	if k.Producer == nil {
		return nil, errProducerUnavailable
	}

	err := k.Producer.Produce(&kafkapkg.Message{
		TopicPartition: kafkapkg.TopicPartition{Topic: &topicN, Partition: kafkapkg.PartitionAny},
		Value:          request.Message,
		Key:            []byte(request.OrderingKey),
		Headers:        carrier,
	}, deliveryChan)
	if err != nil {
		messageBrokerOperationError.WithLabelValues(env, Kafka, "SendMessage", err.Error()).Inc()
		return nil, err
	}

	var m *kafkapkg.Message

	start := time.Now()

	// This is a blocking call. At times due to broker downtimes/back-pressure this call could take a significant amount of time.
	// During which the ELB/ALB could terminate the request. But the thread is still held in-memory until the pod restarts or ack is recieved.
	// This causes memory bloat and results in OOM exceptions. To avoid this we explicitly need to manage the lifecycle of this callstack.
	// 1. Maintain a ticker to ensure that after a certain time the producer gets flushed and the message is not held in buffer.
	// 2. Watch the request context for ctx.Done() and terminate the synchronous wait for the ack event.
	// 3. Ensure that the request throws an error after a certain time period if no ack is received.

	ticker := time.NewTimer(5 * time.Second)

	done := false
	for {
		select {
		case event := <-deliveryChan:
			m = event.(*kafkapkg.Message)
			done = true
		case <-ticker.C:
			logger.Ctx(ctx).Errorw("Callback timeout expired while waiting for ack", "messageId", request.MessageID)
			k.Producer.Flush(1000)
			done = true
		case <-ctx.Done():
			logger.Ctx(ctx).Errorw("Request context was terminated", "messageId", request.MessageID)
			done = true
		}
		if done {
			break
		}
	}

	logger.Ctx(ctx).Infow("successfully received callback", "msgId", request.MessageID, "time taken", time.Since(start).String())

	if m != nil && m.TopicPartition.Error != nil {
		logger.Ctx(ctx).Errorw("kafka: error in publishing messages", "error", m.TopicPartition.Error.Error())
		messageBrokerOperationError.WithLabelValues(env, Kafka, "SendMessage", m.TopicPartition.Error.Error()).Inc()
		return nil, m.TopicPartition.Error
	}

	return &SendMessageToTopicResponse{MessageID: request.MessageID}, nil
}

//ReceiveMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
//from the previous committed offset. If the available messages in the queue are less, returns
// how many ever messages are available
func (k *KafkaBroker) ReceiveMessages(ctx context.Context, request GetMessagesFromTopicRequest) (*GetMessagesFromTopicResponse, error) {

	span, ctx := opentracing.StartSpanFromContext(ctx, "Kafka.ReceiveMessages")
	defer span.Finish()

	messageBrokerOperationCount.WithLabelValues(env, Kafka, "ReceiveMessages").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "ReceiveMessages").Observe(time.Now().Sub(startTime).Seconds())
	}()

	msgs := make([]ReceivedMessage, 0)
	for {
		msg, err := k.Consumer.ReadMessage(time.Duration(request.TimeoutMs) * time.Millisecond)
		if err == nil {
			// extract the message headers and set in the response struct
			receivedMessage := convertKafkaHeadersToResponse(msg.Headers)
			receivedMessage.OrderingKey = string(msg.Key)
			receivedMessage.Data = msg.Value
			receivedMessage.Topic = *msg.TopicPartition.Topic
			receivedMessage.Partition = msg.TopicPartition.Partition
			receivedMessage.Offset = int32(msg.TopicPartition.Offset)
			receivedMessage.OrderingKey = string(msg.Key)

			msgs = append(msgs, receivedMessage)
			if int32(len(msgs)) == request.NumOfMessages {
				return &GetMessagesFromTopicResponse{Messages: msgs}, nil
			}
		} else if err.(kafkapkg.Error).Code() == kafkapkg.ErrTimedOut {
			messageBrokerOperationError.WithLabelValues(env, Kafka, "ReceiveMessages", err.Error()).Inc()
			return &GetMessagesFromTopicResponse{Messages: msgs}, nil
		} else {
			// The client will automatically try to recover from all errors.
			logger.Ctx(ctx).Errorw("kafka: error in receiving messages", "msg", err.Error())
			messageBrokerOperationError.WithLabelValues(env, Kafka, "ReceiveMessages", err.Error()).Inc()
			return nil, err
		}
	}
}

// CommitByPartitionAndOffset Commits messages if any
//This func will commit the message consumed
//by all the previous calls to GetMessages
func (k *KafkaBroker) CommitByPartitionAndOffset(ctx context.Context, request CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Kafka.CommitByPartitionAndOffset")
	defer span.Finish()

	messageBrokerOperationCount.WithLabelValues(env, Kafka, "CommitByPartitionAndOffset").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "CommitByPartitionAndOffset").Observe(time.Now().Sub(startTime).Seconds())
	}()

	logger.Ctx(ctx).Infow("kafka: commit request received", "request", request)

	topicN := normalizeTopicName(request.Topic)
	tp := kafkapkg.TopicPartition{
		Topic:     &topicN,
		Partition: request.Partition,
		Offset:    kafkapkg.Offset(request.Offset),
	}

	tps := make([]kafkapkg.TopicPartition, 0)
	tps = append(tps, tp)

	attempt := 1
	resp, err := k.Consumer.CommitOffsets(tps)

	if resp != nil {
		logger.Ctx(ctx).Infow("kafka: commit attempted", "response", resp)
	}

	for {
		if err != nil && err.Error() == kafkapkg.ErrRequestTimedOut.String() && attempt <= 3 {
			logger.Ctx(ctx).Infow("kafka: retrying commit", "attempt", attempt)
			messageBrokerOperationError.WithLabelValues(env, Kafka, "CommitByPartitionAndOffset", err.Error()).Inc()
			resp, err = k.Consumer.CommitOffsets(tps)
			attempt++
			time.Sleep(time.Millisecond * 100)
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
func (k *KafkaBroker) Pause(ctx context.Context, request PauseOnTopicRequest) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Kafka.Pause")
	defer span.Finish()

	messageBrokerOperationCount.WithLabelValues(env, Kafka, "Pause").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "Pause").Observe(time.Now().Sub(startTime).Seconds())
	}()

	topicN := normalizeTopicName(request.Topic)
	tp := kafkapkg.TopicPartition{
		Topic:     &topicN,
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
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "Resume").Observe(time.Now().Sub(startTime).Seconds())
	}()

	topicN := normalizeTopicName(request.Topic)
	tp := kafkapkg.TopicPartition{
		Topic:     &topicN,
		Partition: request.Partition,
	}

	tps := make([]kafkapkg.TopicPartition, 0)
	tps = append(tps, tp)

	return k.Consumer.Resume(tps)
}

// FetchConsumerLag calculates consumer lag for all assigned partitions
func (k *KafkaBroker) FetchConsumerLag(ctx context.Context) (map[string]uint64, error) {
	lag := make(map[string]uint64)

	assigned, err := k.Consumer.Assignment()
	if err != nil {
		return lag, err
	}

	committed, err := k.Consumer.Committed(assigned, kafkaMetadataTimeout)
	if err != nil {
		return lag, err
	}

	for _, tp := range committed {
		topicPart := *tp.Topic + "=" + strconv.Itoa(int(tp.Partition))
		low, high, err := k.Consumer.QueryWatermarkOffsets(*tp.Topic, tp.Partition, kafkaMetadataTimeout)
		if err != nil {
			logger.Ctx(ctx).Errorw("kafka: failed to fetch watermark offsets",
				"error", err.Error(),
				"topic", tp.Topic,
				"partition", tp.Partition,
			)
			continue
		}
		if tp.Offset == kafkapkg.OffsetInvalid {
			lag[topicPart] = uint64(high - low)
		} else {
			lag[topicPart] = uint64(high - int64(tp.Offset))
		}
	}
	return lag, nil
}

// Close closes the consumer
func (k *KafkaBroker) Close(ctx context.Context) error {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "Close").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "Close").Observe(time.Now().Sub(startTime).Seconds())
	}()

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

// AddTopicPartitions adds partitions to an existing topic
// NOTE: Use with extreme caution! Calling this will result in a consumer-group re-balance and
// could result in undesired behaviour if the topic is being used for ordered messages.
func (k *KafkaBroker) AddTopicPartitions(ctx context.Context, request AddTopicPartitionRequest) (*AddTopicPartitionResponse, error) {
	messageBrokerOperationCount.WithLabelValues(env, Kafka, "AddTopicPartitions").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "AddTopicPartitions").Observe(time.Now().Sub(startTime).Seconds())
	}()

	tp := normalizeTopicName(request.Name)
	metadata, merr := k.Admin.GetMetadata(&tp, false, 1000)
	if merr != nil {
		logger.Ctx(ctx).Errorw("kafka: admin getMetadata() failed", "topic", request.NumPartitions, "error", merr.Error())
		messageBrokerOperationError.WithLabelValues(env, Kafka, "AddTopicPartitions", merr.Error()).Inc()
		return nil, merr
	}

	// fetch the topic metadata and check the current partition count
	if _, ok := metadata.Topics[tp]; !ok {
		// invalid topic
		return nil, fmt.Errorf("topic [%v] not found", tp)
	}

	// metadata contains non-existent topic names as well with partition count as zero
	currPartitionCount := len(metadata.Topics[tp].Partitions)
	if currPartitionCount == 0 {
		// invalid topic
		return nil, fmt.Errorf("topic [%v] not found", tp)
	}

	if request.NumPartitions == currPartitionCount {
		// same partition count, do not error out
		return &AddTopicPartitionResponse{
			Response: nil,
		}, nil
	} else if request.NumPartitions < currPartitionCount {
		// less partition count
		return nil, fmt.Errorf("topic [%v]: new partition count [%v] cannot be less as current than [%v]", tp, request.NumPartitions, currPartitionCount)
	}

	logger.Ctx(ctx).Infow("kafka: request to add topic partitions", "topic", tp,
		"current_partitions", currPartitionCount, "new_partitions", request.NumPartitions)

	ps := kafkapkg.PartitionsSpecification{
		Topic:      tp,
		IncreaseTo: request.NumPartitions,
	}

	pss := make([]kafkapkg.PartitionsSpecification, 0)
	pss = append(pss, ps)

	resp, err := k.Admin.CreatePartitions(ctx, pss, nil)
	if err != nil {
		logger.Ctx(ctx).Errorw("kafka: request to add topic partitions failed", "topic", tp, "numPartitions", request.NumPartitions,
			"error", err.Error())
		return nil, err
	}

	logger.Ctx(ctx).Infow("kafka: request to add topic partitions completed", "topic", tp, "numPartitions", request.NumPartitions,
		"resp", resp)
	return &AddTopicPartitionResponse{
		Response: resp,
	}, nil
}

// IsHealthy checks the health of the kafka
func (k *KafkaBroker) IsHealthy(ctx context.Context) (bool, error) {
	string, err := k.Admin.ClusterID(ctx)
	if string != "" {
		return true, nil
	}

	if ctx.Err() == context.DeadlineExceeded || err == context.DeadlineExceeded {
		return false, errors.New("kafka: timed out")
	}

	return false, err
}

// Shutdown closes the producer
func (k *KafkaBroker) Shutdown(ctx context.Context) {
	// immediately mark the producer as closed so that it is not re-used during the close operation
	k.isProducerClosed = true

	messageBrokerOperationCount.WithLabelValues(env, Kafka, "Shutdown").Inc()

	startTime := time.Now()
	defer func() {
		messageBrokerOperationTimeTaken.WithLabelValues(env, Kafka, "Shutdown").Observe(time.Now().Sub(startTime).Seconds())
	}()

	logger.Ctx(ctx).Infow("kafka: request to close the producer", "topic", k.COptions.Topics)

	if k.Producer != nil {
		k.Producer.Flush(500)
		k.Producer.Close()
		logger.Ctx(ctx).Infow("kafka: producer closed", "topic", k.COptions.Topics)
	}

	logger.Ctx(ctx).Infow("kafka: producer already closed", "topic", k.COptions.Topics)
}

// IsClosed checks if producer has been closed
func (k *KafkaBroker) IsClosed(_ context.Context) bool {
	return k.isProducerClosed
}

// Flush flushes the producer buffer
func (k *KafkaBroker) Flush(timeoutMs int) error {
	if k.Producer == nil {
		return errProducerUnavailable
	}
	k.Producer.Flush(timeoutMs)
	return nil
}
