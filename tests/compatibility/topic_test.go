// +build compatibility

package compatibility

import (
	"context"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func Test_Topic_CreateTopic(t *testing.T) {
	for _, client := range []*pubsub.Client{metroClient, emulatorClient} {
		topic, err := client.CreateTopic(context.Background(), "topic-name-a")
		assert.Nil(t, err)
		assert.NotNil(t, topic)
	}
}
