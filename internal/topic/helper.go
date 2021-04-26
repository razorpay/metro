package topic

import (
	"fmt"
)

const (
	// TopicNameFormat for public topic name "projects/{projectID}/topics/{topicName}
	TopicNameFormat = "projects/%s/topics/%s"
)

// GetTopicName helper return the public topic name using project and topic name using format
func GetTopicName(projectID string, name string) string {
	return fmt.Sprintf(TopicNameFormat, projectID, name)
}
