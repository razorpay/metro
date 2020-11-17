package redis_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goRedis "github.com/go-redis/redis"
	"github.com/razorpay/metro/pkg/worker/queue/broker/redis"
	"github.com/stretchr/testify/assert"
)

var (
	redisClient *goRedis.Client
	redisQueue  redis.Queue
)

func TestMain(m *testing.M) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	redisClient = goRedis.NewClient(&goRedis.Options{
		Addr: s.Addr(),
	})

	redisQueue, _ = redis.NewWithClient("", redisClient)

	os.Exit(m.Run())
}

func TestRedisQueue_Enqueue(t *testing.T) {
	messageIdPush, err := redisQueue.Enqueue("test_body", 0, "test_queue")
	assert.Nil(t, err)

	messageIdActual, err := redisClient.LRange("test_queue", 0, 1).Result()
	assert.Nil(t, err)
	assert.Equal(t, messageIdPush, messageIdActual[0])

	storeKey := redis.GetMessageStoreKey("test_queue")
	message, err := redisClient.HGet(storeKey, messageIdPush).Result()
	assert.Nil(t, err)

	var redisMessage redis.Message

	err1 := json.Unmarshal([]byte(message), &redisMessage)
	assert.Nil(t, err1)
	assert.Equal(t, "test_body", redisMessage.GetBody())
	assert.Equal(t, messageIdPush, redisMessage.GetID())

	keys, err := redisClient.Keys("*").Result()
	assert.Nil(t, err)
	assert.Equal(t, "test_queue", keys[0])
	assert.Equal(t, storeKey, keys[1])

	// this is to clear the queue for further use
	redisClient.FlushAll()
}

func TestRedisQueue_Dequeue(t *testing.T) {
	messageId, err := redisQueue.Enqueue("test_body", 0, "test_queue")
	assert.Nil(t, err)

	messageIdPush, messageBody, err := redisQueue.Dequeue("test_queue")
	assert.Nil(t, err)
	assert.Equal(t, messageIdPush, messageId)
	assert.Equal(t, "test_body", messageBody)
}

func TestRedisQueue_Acknowledge(t *testing.T) {
	messageIdPush1, err := redisQueue.Enqueue("test_body", 0, "test_queue")
	assert.Nil(t, err)
	messageIdPush2, err := redisQueue.Enqueue("test_body_2", 0, "test_queue")
	assert.Nil(t, err)

	err = redisQueue.Acknowledge(messageIdPush1, "test_queue")
	assert.Nil(t, err)

	messageIdActual, err := redisClient.LRange("test_queue", 0, 2).Result()
	assert.Nil(t, err)
	assert.Equal(t, messageIdPush2, messageIdActual[0])

	storeKey := redis.GetMessageStoreKey("test_queue")
	message1, err := redisClient.HGet(storeKey, messageIdPush1).Result()
	assert.Equal(t, goRedis.Nil, err)
	assert.Equal(t, "", message1)

	message2, err := redisClient.HGet(storeKey, messageIdPush2).Result()
	assert.Nil(t, err)

	var redisMessage redis.Message

	err1 := json.Unmarshal([]byte(message2), &redisMessage)
	assert.Nil(t, err1)
	assert.Equal(t, "test_body_2", redisMessage.GetBody())
	assert.Equal(t, messageIdPush2, redisMessage.GetID())

	keys, err := redisClient.Keys("*").Result()
	assert.Nil(t, err)
	assert.Equal(t, "test_queue", keys[0])
	assert.Equal(t, storeKey, keys[1])
}
