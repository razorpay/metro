//go:build unit
// +build unit

package messagebroker

import (
	"testing"

	kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func Test_IsErrorRecoverable(t *testing.T) {
	assert.True(t, IsErrorRecoverable(nil))
	assert.False(t, IsErrorRecoverable(errors.New("some random error")))

	assert.True(t, IsErrorRecoverable(kafkapkg.NewError(kafkapkg.ErrTimedOut, "", false)))
	assert.False(t, IsErrorRecoverable(kafkapkg.NewError(kafkapkg.ErrAllBrokersDown, "", true)))
}
