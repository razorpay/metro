package topic

import (
	"fmt"

	"github.com/razorpay/metro/internal/common"
)

const (
	// TopicNameFormat for public topic name "metro-{env}/projects/{projectID}/topics/{topicName}
	TopicNameFormat = "%s/projects/%s/topics/%s"
)

// GetTopicName helper return the public topic name using project and topic name using format
func GetTopicName(projectID string, name string) string {
	return fmt.Sprintf(TopicNameFormat, common.GetBasePrefix(), projectID, name)
}
