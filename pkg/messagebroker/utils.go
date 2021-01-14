package messagebroker

import "strings"

func normalizeTopicName(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}
