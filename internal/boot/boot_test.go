package boot

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoot_NewContext_Nil(t *testing.T) {
	ctx := NewContext(nil)
	assert.NotNil(t, ctx)
}

func TestBoot_NewContext_NotNil(t *testing.T) {
	ctxIn := context.Background()
	ctxOut := NewContext(ctxIn)
	assert.NotNil(t, ctxOut)
	assert.Equal(t, ctxIn, ctxOut)
}
