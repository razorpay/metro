// +build unit

package patch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_PatchStruct(t *testing.T) {
	ctx := context.Background()

	type structA struct {
		A int
		B string
		C map[string]interface{}
		D []string
	}

	a := structA{
		A: 10,
		B: "abcd",
		C: map[string]interface{}{"a": "b"},
		D: []string{"1", "2"},
	}
	b := structA{
		A: 20,
		B: "xyz",
		C: map[string]interface{}{"x": "y"},
		D: []string{"a", "b"},
	}
	pExpect := structA{
		A: 10,
		B: "abcd",
		C: map[string]interface{}{"x": "y"},
		D: []string{"a", "b"},
	}
	StructPatch(ctx, &b, &a, []string{"C", "D"})
	assert.Equal(t, pExpect, a, "Wrong patching")
}

func Test_PatchStructMismatchedTypes(t *testing.T) {
	ctx := context.Background()

	type structA struct {
		A int
		B string
		C map[string]interface{}
		D []string
	}

	type structB struct {
		A int
		B string
		C map[string]interface{}
		D []string
	}

	a := structA{
		A: 10,
		B: "abcd",
		C: map[string]interface{}{"a": "b"},
		D: []string{"1", "2"},
	}
	b := structB{
		A: 20,
		B: "xyz",
		C: map[string]interface{}{"x": "y"},
		D: []string{"a", "b"},
	}
	err := StructPatch(ctx, &b, &a, []string{"C", "D"})
	assert.NotNil(t, err)
}

func Test_PatchStructNonPointers(t *testing.T) {
	ctx := context.Background()

	type structA struct {
		A int
		B string
		C map[string]interface{}
		D []string
	}

	a := structA{
		A: 10,
		B: "abcd",
		C: map[string]interface{}{"a": "b"},
		D: []string{"1", "2"},
	}
	b := structA{
		A: 20,
		B: "xyz",
		C: map[string]interface{}{"x": "y"},
		D: []string{"a", "b"},
	}

	err := StructPatch(ctx, b, a, []string{"C", "D"})
	assert.NotNil(t, err)
}
