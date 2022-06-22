package retry

import (
	"encoding/base64"
	"strconv"
	"time"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
)

const (
	retryCountPrefix = "retry-count-"
)

func (dc *DelayConsumer) fetchRetryCount(msg messagebroker.ReceivedMessage) (int, error) {

	key := getKey(msg)
	logger.Ctx(dc.ctx).Infow("fetching retry count", "key", key)
	val, err := dc.ch.Get(dc.ctx, key)
	if err != nil {
		logger.Ctx(dc.ctx).Errorw("error fetching retry count", "key", key, "msg", err.Error())
		return 0, err
	}

	count, err := strconv.Atoi(string(val))
	if err != nil {
		logger.Ctx(dc.ctx).Errorw("error fetching retry count", "key", key, "msg", err.Error())
		return count, err
	}

	return count, nil
}

func (dc *DelayConsumer) incrementRetryCount(msg messagebroker.ReceivedMessage) error {
	logger.Ctx(dc.ctx).Infow("Counter: Incrementing retry count for message", "msgId", msg.MessageID, "count", msg.CurrentRetryCount)
	countToUpdate := 0
	count, err := dc.fetchRetryCount(msg)
	if err != nil {
		logger.Ctx(dc.ctx).Errorw("Counter: Failed to fetch retry count for update", "cache count", count, "msg", err.Error())
		countToUpdate = int(msg.CurrentRetryCount + 1)
	} else {
		if int(msg.CurrentRetryCount) > count {
			countToUpdate = int(msg.CurrentRetryCount + 1)
		} else {
			countToUpdate = count + 1
		}
	}

	logger.Ctx(dc.ctx).Infow("updating retry count", "msgID", msg.MessageID, "newCount", countToUpdate, "topic", msg.CurrentTopic)

	// Set a 12 hour TTl since most messages do not require more than 12 hours of retry.
	err = dc.ch.Set(dc.ctx, getKey(msg), []byte(strconv.Itoa(countToUpdate)), time.Duration(12*time.Hour))
	if err != nil {
		return err
	}
	return nil
}

func (dc *DelayConsumer) deleteRetryCount(msg messagebroker.ReceivedMessage) error {

	key := getKey(msg)

	err := dc.ch.Delete(dc.ctx, key)
	if err != nil {
		return err
	}
	return nil
}

func getKey(msg messagebroker.ReceivedMessage) string {
	sub := msg.Subscription
	key := retryCountPrefix + base64.StdEncoding.EncodeToString([]byte(sub)) + "-" + msg.MessageID
	return key
}
