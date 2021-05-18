// +build functional

package functional

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_PubSubPush(t *testing.T) {
	t.SkipNow()
	// create a topic
	topicName := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	topic, err := client.CreateTopic(context.Background(), topicName)
	assert.Nil(t, err)
	assert.NotNil(t, topic)

	// create a push subscription
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])
	sub, err := client.CreateSubscription(context.Background(), subscription,
		pubsub.SubscriptionConfig{
			Topic: topic,
			PushConfig: pubsub.PushConfig{
				Endpoint: "http://localhost:8099/push",
			},
		})
	if err != nil {
		t.Logf("error in create subscription: %s", err)
	}
	assert.Nil(t, err)

	// Adding some delay for subscription scheduling
	// Explore solutions
	// 1. Subscription State
	// 2. Node Binding API
	// 3. Read from registry directly in tests
	time.Sleep(time.Duration(time.Second * 5))

	numOfMessages := 2
	processedTotal := 0
	failedCount := 0
	publishMap := map[string]bool{}

	ctx := context.Background()

	// publish messages in a go routine
	go func() {
		//topic.EnableMessageOrdering = true
		for i := 0; i < numOfMessages; i++ {
			text := fmt.Sprintf("payload %d", i)
			publishMap[text] = false
			r := topic.Publish(ctx, &pubsub.Message{Data: []byte(text)})
			r.Get(ctx)
		}
		topic.Stop()
	}()

	for {
		t.Logf("waiting for new request")
		req := <-reqChan
		t.Logf("new request")
		res, text, success := processReq(req, 0)
		respChan <- res

		if !success {
			failedCount++
		} else {
			publishMap[text] = true
		}

		processedTotal++

		if processedTotal == numOfMessages+failedCount {
			break
		}
	}

	for _, v := range publishMap {
		assert.Equal(t, true, v)
	}

	// cleanup
	err = topic.Delete(ctx)
	assert.Nil(t, err)
	err = sub.Delete(ctx)
	assert.Nil(t, err)
}

func processReq(req *http.Request, errorRate int) (*http.Response, string, bool) {
	randomVal := rand.Intn(100)

	reqbody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		randomVal = errorRate - 1
	}

	body := "response"
	if randomVal < errorRate {
		return &http.Response{
			StatusCode:    http.StatusBadRequest,
			Status:        http.StatusText(http.StatusBadRequest),
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			Body:          ioutil.NopCloser(bytes.NewBufferString(body)),
			ContentLength: int64(len(body)),
			Request:       req,
			Header:        make(http.Header, 0),
		}, string(reqbody), false
	}

	return &http.Response{
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          ioutil.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
		Request:       req,
		Header:        make(http.Header, 0),
	}, string(reqbody), true
}
