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

// GetTopicNameOnly from the complete Topic Name
func GetTopicNameOnly(topicName string) string {
	return strings.Split(topicName, "/")[3]
}

// IsRetentionPolicyUnchanged checks if the existing and the required retention policy are same or not
func IsRetentionPolicyUnchanged(existing, required map[string]string) bool {
	return existing[RetentionPeriodConfig] == required[RetentionPeriodConfig] &&
		existing[RetentionSizeConfig] == required[RetentionSizeConfig]
}
