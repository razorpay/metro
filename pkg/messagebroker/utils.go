package messagebroker

import (
	"strings"
)

// NormalizeTopicName returns the actual topic name used in message broker
func NormalizeTopicName(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}
