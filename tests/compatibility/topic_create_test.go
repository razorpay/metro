// +build compatibility

package compatibility

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_Topic_CreateTopic(t *testing.T) {
	topic := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])

	ctx := context.Background()
	metroTopic, err := metroClient.CreateTopic(ctx, topic)
	assert.Nil(t, err)

	emulatorTopic, err := emulatorClient.CreateTopic(ctx, topic)
	assert.Nil(t, err)

	assert.Equal(t, metroTopic.ID(), emulatorTopic.ID())
	assert.Equal(t, metroTopic.String(), emulatorTopic.String())
	assert.Equal(t, metroTopic.PublishSettings.Timeout, emulatorTopic.PublishSettings.Timeout)
	assert.Equal(t, metroTopic.PublishSettings.DelayThreshold, emulatorTopic.PublishSettings.DelayThreshold)

	// cleanup
	err = emulatorTopic.Delete(ctx)
	assert.Nil(t, err)
	err = metroTopic.Delete(ctx)
	assert.Nil(t, err)
}

func Test_Topic_CreateTopic_AlreadyExists(t *testing.T) {
	topic := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])

	ctx := context.Background()
	metroTopicA, errA := metroClient.CreateTopic(ctx, topic)
	assert.Nil(t, errA)

	emulatorTopicA, errB := emulatorClient.CreateTopic(ctx, topic)
	assert.Nil(t, errB)

	metroTopicB, errC := metroClient.CreateTopic(ctx, topic)
	assert.NotNil(t, errC)
	assert.Nil(t, metroTopicB)

	emulatorTopicB, errD := emulatorClient.CreateTopic(ctx, topic)
	assert.NotNil(t, errD)
	assert.Nil(t, emulatorTopicB)

	assert.Equal(t, errD.Error(), errC.Error())

	// cleanup
	err := emulatorTopicA.Delete(ctx)
	assert.Nil(t, err)
	err = metroTopicA.Delete(ctx)
	assert.Nil(t, err)
}

func Test_Topic_CreateTopic_InvalidTopicName_1(t *testing.T) {
	ctx := context.Background()
	metroTopic, errA := metroClient.CreateTopic(ctx, "t")
	assert.Nil(t, metroTopic)
	assert.NotNil(t, errA)

	emulatorTopic, errB := emulatorClient.CreateTopic(ctx, "t")
	assert.Nil(t, emulatorTopic)
	assert.NotNil(t, errB)

	assert.Equal(t, errB.Error(), errA.Error())
}

func Test_Topic_CreateTopic_InvalidTopicName_2(t *testing.T) {
	ctx := context.Background()
	metroTopic, errA := metroClient.CreateTopic(ctx, "goog-abc")
	assert.Nil(t, metroTopic)
	assert.NotNil(t, errA)

	emulatorTopic, errB := emulatorClient.CreateTopic(ctx, "goog-abc")
	assert.Nil(t, emulatorTopic)
	assert.NotNil(t, errB)

	assert.Equal(t, errB.Error(), errA.Error())
}

func Test_Topic_CreateTopic_InvalidTopicName_3(t *testing.T) {
	ctx := context.Background()
	metroTopic, errA := metroClient.CreateTopic(ctx, "goog-$-abc")
	assert.Nil(t, metroTopic)
	assert.NotNil(t, errA)

	emulatorTopic, errB := emulatorClient.CreateTopic(ctx, "goog-$-abc")
	assert.Nil(t, emulatorTopic)
	assert.NotNil(t, errB)

	assert.Equal(t, errB.Error(), errA.Error())
}

func Test_Topic_CreateTopic_InvalidTopicName_4(t *testing.T) {
	ctx := context.Background()
	metroTopic, errA := metroClient.CreateTopic(ctx, "_goog-$-abc")
	assert.Nil(t, metroTopic)
	assert.NotNil(t, errA)

	emulatorTopic, errB := emulatorClient.CreateTopic(ctx, "_goog-$-abc")
	assert.Nil(t, emulatorTopic)
	assert.NotNil(t, errB)

	assert.Equal(t, errB.Error(), errA.Error())
}
