package stream

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/panjf2000/ants/v2"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type deliveryStatus struct {
	msg    *metrov1.ReceivedMessage
	status bool
}
type processor struct {
	ctx          context.Context
	msgChan      chan *metrov1.ReceivedMessage
	statusChan   chan deliveryStatus
	subID        string
	subscription *subscription.Model
	httpClient   *http.Client
	pool         *ants.PoolWithFunc
}

func (pr *processor) Printf(log string, args ...interface{}) {
	logger.Ctx(pr.ctx).Infow(log, args)
}
func newProcessor(ctx context.Context, poolSize int, msgChan chan *metrov1.ReceivedMessage, statusChan chan deliveryStatus, subID string, sub *subscription.Model, httpClient *http.Client) *processor {
	pr := &processor{
		ctx:          ctx,
		msgChan:      msgChan,
		statusChan:   statusChan,
		subID:        subID,
		subscription: sub,
		httpClient:   httpClient,
	}
	pool, err := ants.NewPoolWithFunc(poolSize, func(i interface{}) {
		msg := i.(*metrov1.ReceivedMessage)
		success := pr.pushMessage(pr.ctx, msg)
		pr.statusChan <- deliveryStatus{
			msg,
			success,
		}
	},
		ants.WithLogger(pr),
		ants.WithPreAlloc(false),
		//ants.WithMaxBlockingTasks(1),
	)
	if err != nil {
		logger.Ctx(ctx).Errorw("processor: Failed to set up porcessor pool", "subId", subID, "subscription", sub.Name)
	}
	pr.pool = pool
	return pr
}
func (pr *processor) start() {
	logger.Ctx(pr.ctx).Infow("processor started")
	for {
		select {
		case msg := <-pr.msgChan:
			logger.Ctx(pr.ctx).Infow("Received message at processor", "msgId", msg.Message.MessageId)
			pr.pool.Invoke(msg)
		case <-pr.ctx.Done():
			return
		}
	}
}

func (pr *processor) pushMessage(ctx context.Context, message *metrov1.ReceivedMessage) bool {
	subModel := pr.subscription
	logFields := make(map[string]interface{})
	logFields["subscripitonId"] = pr.subID
	logFields["subscription"] = pr.subscription.Name
	logFields["messageId"] = message.Message.MessageId
	logFields["ackId"] = message.AckId
	logger.Ctx(ctx).Infow("worker: publishing response data to subscription endpoint", "logFields", logFields)
	span, ctx := opentracing.StartSpanFromContext(ctx, "PushStream.PushMessage", opentracing.Tags{
		"subscriber":   pr.subID,
		"subscription": pr.subscription.Name,
		"topic":        pr.subscription.Topic,
		"message_id":   message.Message.MessageId,
	})
	defer span.Finish()

	startTime := time.Now()
	pushRequest := newPushEndpointRequest(message, subModel.Name)
	postData := getRequestBytes(pushRequest)
	req, err := http.NewRequest(http.MethodPost, subModel.PushConfig.PushEndpoint, postData)
	if err != nil {
		logger.Ctx(ctx).Errorw("processor: Failed to post message to endpoint", "subscription", pr.subscription.Name, "msgId", message.Message.MessageId)
	}
	req.Header.Set("Content-Type", "application/json")
	if subModel.HasCredentials() {
		req.SetBasicAuth(subModel.GetCredentials().GetUsername(), subModel.GetCredentials().GetPassword())
	}

	if span != nil {
		opentracing.GlobalTracer().Inject(
			span.Context(),
			opentracing.HTTPHeaders,
			opentracing.HTTPHeadersCarrier(req.Header))
	}

	logFields["endpoint"] = subModel.PushConfig.PushEndpoint
	logger.Ctx(ctx).Infow("worker: posting messages to subscription url", "logFields", logFields)
	resp, err := pr.httpClient.Do(req)

	// log metrics
	workerPushEndpointCallsCount.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushConfig.PushEndpoint, pr.subID).Inc()
	workerPushEndpointTimeTaken.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushConfig.PushEndpoint).Observe(time.Now().Sub(startTime).Seconds())

	// Process responnse
	if err != nil {
		logger.Ctx(ctx).Errorw("worker: error posting messages to subscription url", "logFields", logFields, "error", err.Error())
		return false
	}

	logger.Ctx(pr.ctx).Infow("worker: push response received for subscription", "status", resp.StatusCode, "logFields", logFields)
	workerPushEndpointHTTPStatusCode.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushConfig.PushEndpoint, fmt.Sprintf("%v", resp.StatusCode)).Inc()

	success := false
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		success = true
	}

	// discard response.Body after usage and ignore errors
	if !success {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Ctx(pr.ctx).Errorw("worker: push was unsuccessful and could not read response body", "status", resp.StatusCode, "logFields", logFields, "error", err.Error())
		} else {
			logger.Ctx(pr.ctx).Errorw("worker: push was unsuccessful", "status", resp.StatusCode, "body", string(bodyBytes), "logFields", logFields)
		}
	}
	_, err = io.Copy(ioutil.Discard, resp.Body)
	err = resp.Body.Close()
	if err != nil {
		logger.Ctx(pr.ctx).Errorw("worker: push response error on response io close()", "status", resp.StatusCode, "logFields", logFields, "error", err.Error())
	}

	return success
}

func (pr *processor) Shutdown() {
	ants.Release()
}
