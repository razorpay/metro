package topic

import (
	"fmt"
	"strings"
)

const (
	// TopicNameFormat for public topic name "projects/{projectID}/topics/{topicName}
	TopicNameFormat = "projects/%s/topics/%s"
)

// GetTopicName helper return the public topic name using project and topic name using format
func GetTopicName(projectID string, name string) string {
	return fmt.Sprintf(TopicNameFormat, projectID, name)
}

// IsDLQTopic helper checks if the topic is dlq topic
func IsDLQTopic(topicName string) bool {
	return strings.HasSuffix(topicName, DeadLetterTopicSuffix)
}
