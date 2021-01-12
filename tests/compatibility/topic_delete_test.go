// +build compatibility

package compatibility

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Topic_DeleteTopic(t *testing.T) {
	ctx := context.Background()
	metroTopic, err := metroClient.CreateTopic(ctx, "topic-name-a")
	assert.Nil(t, err)

	emulatorTopic, err := emulatorClient.CreateTopic(ctx, "topic-name-a")
	assert.Nil(t, err)

	err = metroTopic.Delete(ctx)
	assert.Nil(t, err)

	err = emulatorTopic.Delete(ctx)
	assert.Nil(t, err)
}

func Test_Topic_DeleteTopic_NotFound(t *testing.T) {
	ctx := context.Background()
	// happy case
	metroTopic, err := metroClient.CreateTopic(ctx, "topic-name-a")
	assert.Nil(t, err)

	// happy case
	emulatorTopic, err := emulatorClient.CreateTopic(ctx, "topic-name-a")
	assert.Nil(t, err)

	// happy case
	err = metroTopic.Delete(ctx)
	assert.Nil(t, err)

	// happy case
	err = emulatorTopic.Delete(ctx)
	assert.Nil(t, err)

	// test case for deleted topic
	errA := emulatorTopic.Delete(ctx)
	assert.NotNil(t, errA)

	errB := metroTopic.Delete(ctx)
	assert.NotNil(t, errB)

	assert.Equal(t, errA.Error(), errB.Error())
}
