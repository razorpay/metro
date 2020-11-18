package redis

import (
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/rs/xid"
)

const (
	// Dialect identifier for current dialect
	Dialect = "redis"
)

// Config holds all required info for initializing redis driver
type Config struct {
	Host           string
	Port           int32
	Database       int32
	QueueBatchSize int32
	Password       string
	Client         *redis.Client
}

// Message holds the message payload
type Message struct {
	Body string
	ID   string
}

// Queue holds the handler for the redisclient and auxiliary info
type Queue struct {
	name        string
	redisClient *redis.Client
	batchSize   int32
}

// New inits a Queue driver
func New(queueName string, config *Config) (Queue, error) {

	// If client is provided use it rather than creating a new one
	if config.Client != nil {
		return NewWithClient(queueName, config.Client)
	}

	options := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Password: config.Password,
		DB:       int(config.Database),
	}
	redisClient := redis.NewClient(options)

	return NewWithClient(queueName, redisClient)
}

// NewWithClient returns the queue instance with already initialize client
func NewWithClient(queueName string, client *redis.Client) (Queue, error) {
	var queue Queue

	_, err := client.Ping().Result()

	if err != nil {
		return queue, err
	}

	queue = Queue{
		name:        queueName,
		redisClient: client,
		// need to check the usage of it
		batchSize: 0,
	}

	return queue, nil
}

// Enqueue adds a message to the queue
func (q Queue) Enqueue(message string, delay int64, queueName string) (string, error) {
	rm := Message{Body: message, ID: xid.New().String()}

	pipeline := q.redisClient.TxPipeline()

	encodedMessage, err := json.Marshal(&rm)

	if err != nil {
		return "", err
	}

	messageStoreKey := GetMessageStoreKey(queueName)

	pipeline.HSet(messageStoreKey, rm.ID, string(encodedMessage))

	pipeline.LPush(queueName, rm.ID)

	_, err = pipeline.Exec()

	return rm.ID, err
}

// Dequeue fetches a set of messages from the queue
func (q Queue) Dequeue(queueName string) (string, string, error) {
	var message string

	id, err := q.redisClient.RPopLPush(queueName, queueName).Result()

	if err != nil {
		// in case of nil dont throw error as worker manager will treat this as
		// empty queue message
		if err == redis.Nil {
			return "", "", nil
		}

		return "", "", err
	}

	messageStoreKey := GetMessageStoreKey(queueName)

	message, err = q.redisClient.HGet(messageStoreKey, id).Result()

	if err != nil {
		// in case of nil dont throw error as worker manager will treat this as
		// empty queue message
		if err == redis.Nil {
			return "", "", nil
		}

		return "", "", err
	}

	var redisMessage Message

	err = json.Unmarshal([]byte(message), &redisMessage)

	if err != nil {
		return "", "", err
	}

	return id, redisMessage.GetBody(), err
}

// GetBody returns the body of the message
func (m *Message) GetBody() string {
	return m.Body
}

// GetID returns the ID of the message
func (m *Message) GetID() string {
	return m.ID
}

// Acknowledge this will basically deletes the message from the redis queue
func (q Queue) Acknowledge(id string, queueName string) error {
	pipeline := q.redisClient.TxPipeline()

	pipeline.LRem(queueName, 1, id)

	messageStoreKey := GetMessageStoreKey(queueName)

	pipeline.HDel(messageStoreKey, id)

	_, err := pipeline.Exec()

	return err
}

// GetMessageStoreKey returns the hash key where the message needs to store
func GetMessageStoreKey(queueName string) string {
	return fmt.Sprintf("{message_store}_%s", queueName)
}
