package credentials

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

func TestRepo_List(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	r := NewRepo(mockRegistry)
	dummyModel := getDummyModel()

	mockRegistry.EXPECT().List(gomock.Any(), "prefix").Return(getDummyRegistryPairs(dummyModel), nil)
	got, err := r.List(ctx, "prefix")
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(got, []common.IModel{dummyModel}))
}

func getDummyModel() *Model {
	model := &Model{Username: "username", Password: "password", ProjectID: "project_id"}
	model.SetVersion("version")
	return model
}

func getDummyRegistryPairs(model common.IModel) []registry.Pair {
	value, _ := json.Marshal(model)
	return []registry.Pair{
		{
			Value:   value,
			Version: model.GetVersion(),
		},
	}
}
