//go:build unit
// +build unit

package stream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/jsonpb"
	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/subscriber"
	mocks2 "github.com/razorpay/metro/internal/subscriber/mocks"
	mocks3 "github.com/razorpay/metro/internal/subscriber/mocks"
	"github.com/razorpay/metro/internal/subscription"
	mocks1 "github.com/razorpay/metro/internal/subscription/mocks/core"
	"github.com/razorpay/metro/pkg/httpclient"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	subID      string = "subscriber-id"
	subName    string = "subscription-name"
	topic      string = "primary-topic"
	retryTopic string = "retry-topic"
	partition  int32  = 0
)

var msgData = []struct {
	data      string
	offset    int32
	messageID string
	ackID     string
}{
	{
		data:      "hello",
		offset:    1,
		messageID: "1",
		ackID:     getAckID(1, "1"),
	},
	{
		data:      "there",
		offset:    2,
		messageID: "2",
		ackID:     getAckID(2, "2"),
	},
}

func getAckID(offset int32, messageID string) string {
	ackDeadline := time.Now().Add(1 * time.Second).Unix()
	ackMessage, _ := subscriber.NewAckMessage(subID, topic, partition, offset, int32(ackDeadline), messageID)
	return ackMessage.BuildAckID()
}

func Test_ProtoMarhshalUnMarshal(t *testing.T) {

	const layout = "Jan 2, 2006 3:04pm"
	tm, _ := time.Parse(layout, "Jan 1, 2021 12:05pm") // using fixed time for deterministic test results

	originalReq := &metrov1.PushEndpointRequest{
		Message: &metrov1.PubsubMessage{
			Data: []byte("abc"),
			Attributes: map[string]string{
				"k1": "v1",
				"k2": "v2",
			},
			MessageId:   "msg-1",
			PublishTime: timestamppb.New(tm),
			OrderingKey: "ok-1",
		},
		Subscription: "subs-1",
	}

	// marshal
	reqBytes := getRequestBytes(originalReq)

	// unmarshal
	currentReq := metrov1.PushEndpointRequest{}
	jsonpb.Unmarshal(reqBytes, &currentReq)

	assert.Equal(t, originalReq.Subscription, currentReq.Subscription)
	assert.Equal(t, originalReq.Message, currentReq.Message)
}

func TestNewPushStream(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	subscriptionCoreMock := mocks1.NewMockICore(ctrl)
	subscriberCoreMock := mocks2.NewMockICore(ctrl)
	subscriptionCoreMock.EXPECT().Get(gomock.Any(), subName).Return(getMockSubModel(""), nil)

	workerID := uuid.New().String()
	httpConfig := &httpclient.Config{}
	got, err := NewPushStream(ctx, workerID, subName, subscriptionCoreMock, subscriberCoreMock, httpConfig)
	assert.NoError(t, err)
	assert.NotNil(t, got)
}

func TestPushStream_Start(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"IsSuccess": true}`))
	}))

	defer s.Close()

	tests := []struct {
		endpoint  string
		isSuccess bool
	}{
		{
			endpoint:  s.URL,
			isSuccess: true,
		},
		{
			endpoint:  "",
			isSuccess: false,
		},
	}

	for _, test := range tests {
		ps := getMockPushStream(ctx, ctrl, test.endpoint)
		go ps.Start()
		<-time.NewTicker(1 * time.Second).C
		ps.cancelFunc()
		close(ps.subs.GetAckChannel())
		close(ps.subs.GetModAckChannel())

		got := make([]string, 0, 10)
		if test.isSuccess {
			for ackMsg := range ps.subs.GetAckChannel() {
				got = append(got, ackMsg.AckID)
			}
		} else {
			for modAck := range ps.subs.GetModAckChannel() {
				got = append(got, modAck.AckMessage.AckID)
			}
		}

		expected := make([]string, 0, len(msgData))
		for _, msg := range msgData {
			expected = append(expected, msg.ackID)
		}
		sort.Strings(got)
		sort.Strings(expected)
		if !reflect.DeepEqual(got, expected) {
			t.Errorf("Start() = %v, want %v", got, expected)
		}
	}
}

func getMockSubModel(endpoint string) *subscription.Model {
	return &subscription.Model{
		Topic:              topic,
		Name:               subName,
		AckDeadlineSeconds: 2,
		PushConfig: &subscription.PushConfig{
			PushEndpoint: endpoint,
		},
	}
}

func getMockPushStream(ctx context.Context, ctrl *gomock.Controller, endpoint string) *PushStream {
	subscriptionCoreMock := mocks1.NewMockICore(ctrl)
	subscriberCoreMock := mocks2.NewMockICore(ctrl)
	subModel := getMockSubModel(endpoint)
	subscriptionCoreMock.EXPECT().Get(gomock.Any(), subName).Return(subModel, nil)
	workerID := uuid.New().String()
	httpConfig := &httpclient.Config{}
	pushStream, _ := NewPushStream(ctx, workerID, subName, subscriptionCoreMock, subscriberCoreMock, httpConfig)
	pushStream.subs = getMockSubscriber(ctx, ctrl)

	subscriberCoreMock.EXPECT().NewSubscriber(
		ctx,
		workerID,
		subModel,
		defaultTimeoutMs,
		defaultMaxOutstandingMsgs,
		defaultMaxOuttandingBytes,
		gomock.AssignableToTypeOf(make(chan *subscriber.PullRequest)),
		gomock.AssignableToTypeOf(make(chan *subscriber.AckMessage)),
		gomock.AssignableToTypeOf(make(chan *subscriber.ModAckMessage))).Return(getMockSubscriber(ctx, ctrl), nil)
	return pushStream
}

func getMockSubscriber(ctx context.Context, ctrl *gomock.Controller) *mocks3.MockISubscriber {
	subscriberMock := mocks3.NewMockISubscriber(ctrl)
	reqCh := make(chan *subscriber.PullRequest, 1)
	resCh := make(chan *metrov1.PullResponse)

	cancelChan := make(chan bool)

	go func() {
		counter := 0
		for {
			select {
			case <-reqCh:
				if counter < 1 {
					messages := getMockResponseMessages()
					resCh <- &metrov1.PullResponse{
						ReceivedMessages: messages,
					}
					counter++
				}
				resCh <- &metrov1.PullResponse{}
			case <-cancelChan:
				return
			}
		}
	}()
	subscriberMock.EXPECT().GetID().AnyTimes()
	subscriberMock.EXPECT().GetErrorChannel().AnyTimes()
	subscriberMock.EXPECT().GetModAckChannel().Return(make(chan *subscriber.ModAckMessage, 10)).AnyTimes()
	subscriberMock.EXPECT().GetAckChannel().Return(make(chan *subscriber.AckMessage, 10)).AnyTimes()
	subscriberMock.EXPECT().GetRequestChannel().Return(reqCh).AnyTimes()
	subscriberMock.EXPECT().GetResponseChannel().Return(resCh).AnyTimes()
	subscriberMock.EXPECT().Stop().Do(func() {
		cancelChan <- true
		close(resCh)
	}).AnyTimes()
	return subscriberMock
}

func getMockResponseMessages() []*metrov1.ReceivedMessage {
	messages := make([]*metrov1.ReceivedMessage, 0, 2)
	for _, msg := range msgData {
		messages = append(messages, &metrov1.ReceivedMessage{
			Message: &metrov1.PubsubMessage{Data: []byte(msg.data)},
			AckId:   msg.ackID,
		})
	}
	return messages
}

func TestPushStream_Stop(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ps := getMockPushStream(ctx, ctrl, "")
	go ps.Start()
	err := ps.Stop()
	assert.Nil(t, err)
}
