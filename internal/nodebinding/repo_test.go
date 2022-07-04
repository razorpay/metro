//go:build unit
// +build unit

package nodebinding

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

func TestRepo_NewRepo(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	r := NewRepo(mockRegistry)
	assert.NotNil(t, r)
}

func TestRepo_List(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	nodeBinding := getDummyNodeBindingModel()
	nodeBinding.SetVersion("version")

	tests := []struct {
		input   []registry.Pair
		want    []common.IModel
		wantErr bool
	}{
		{
			input:   []registry.Pair{getDummyRegistryPair(nodeBinding)},
			want:    []common.IModel{nodeBinding},
			wantErr: false,
		},
		{
			input:   nil,
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		var err error
		if tt.wantErr {
			err = fmt.Errorf("Something went wrong")
		}
		mockRegistry.EXPECT().List(gomock.Any(), "prefix").Return(tt.input, err)
		r := NewRepo(mockRegistry)
		got, err := r.List(ctx, "prefix")
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("List got %v, want %v", got, tt.want)
		}
		assert.Equal(t, tt.wantErr, err != nil)
	}

}

func getDummyRegistryPair(nodeBinding common.IModel) registry.Pair {
	value, _ := json.Marshal(nodeBinding)
	return registry.Pair{
		Key:     "key",
		Value:   []byte(value),
		Version: nodeBinding.GetVersion(),
	}
}
