//go:build unit
// +build unit

package partitionlocker

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewPartitionLocker(t *testing.T) {
	pl := NewPartitionLocker(&sync.Mutex{})
	id := "id007"
	pl.Lock(id)
	assert.True(t, pl.locked(id))
	pl.Unlock(id)
	assert.False(t, pl.locked(id))
	assert.False(t, pl.locked("random-id"))
}
