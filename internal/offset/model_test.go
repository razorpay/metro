package offset

import (
	"strconv"
	"testing"

	"github.com/razorpay/metro/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	offset := getDummyOffsetModel()
	assert.Equal(t, offset.Prefix(), common.GetBasePrefix()+"offsets/")
}

func TestModel_Key(t *testing.T) {
	offset := getDummyOffsetModel()
	assert.Equal(t, offset.Key(), offset.Prefix()+offset.Topic+subscriptionPrefix+offset.Subscription+partitionPrefix+strconv.Itoa(int(offset.Partition))+orderingPrefix+offset.OrderingKey)

	offset.OrderingKey = ""
	assert.Equal(t, offset.Key(), offset.Prefix()+offset.Topic+subscriptionPrefix+offset.Subscription+partitionPrefix+strconv.Itoa(int(offset.Partition)))
}

func getDummyOffsetModel() *Model {
	return &Model{
		Topic:        "topic123",
		Partition:    0,
		Subscription: "sub123",
		LatestOffset: 2,
		OrderingKey:  "abcdef",
	}
}
