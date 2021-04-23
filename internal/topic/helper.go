package topic

import (
	"fmt"

	"github.com/razorpay/metro/internal/common"
)

const (
	TopicNameFormat = "%s/projects/%s/topics/%s"
)

func GetTopicName(projectId string, name string) string {
	return fmt.Sprintf(TopicNameFormat, common.GetBasePrefix(), projectId, name)
}
