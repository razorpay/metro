//go:build unit
// +build unit

package node

import (
	"context"
	"encoding/json"
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
	r := NewRepo(mockRegistry)
	dummyModel := getDummyModel()

	mockRegistry.EXPECT().List(gomock.Any(), "prefix").Return(getDummyRegistryPairs(dummyModel), nil)
	got, err := r.List(ctx, "prefix")
	assert.NoError(t, err)
	if !reflect.DeepEqual(got, []common.IModel{dummyModel}) {
		t.Errorf("List got %v, want %v", got, []common.IModel{dummyModel})
	}
}

func getDummyRegistryPairs(model common.IModel) []registry.Pair {
	value, _ := json.Marshal(model)
	return []registry.Pair{
		{
			Value:   []byte(value),
			Version: model.GetVersion(),
		},
	}
}

func getDummyModel() *Model {
	model := &Model{ID: "1"}
	model.SetVersion("version")
	return model
}
