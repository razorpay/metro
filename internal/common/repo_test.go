// +build unit

package common

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

type sampleModel struct{}

func (s *sampleModel) Key() string { return "sample-key" }

func (s *sampleModel) Prefix() string { return "sample-prefix" }

func TestBaseRepo_Save(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	repo := &BaseRepo{mockRegistry}
	ctx := context.Background()
	mockRegistry.EXPECT().Put(gomock.Any(), "sample-key", []byte("{}"))
	err := repo.Save(ctx, &sampleModel{})
	assert.Nil(t, err)
}

func TestBaseRepo_Acquire(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	repo := &BaseRepo{mockRegistry}
	ctx := context.Background()
	mockRegistry.EXPECT().Acquire(gomock.Any(), "id", "sample-key", []byte("{}")).Return(true, nil)
	err := repo.Acquire(ctx, &sampleModel{}, "id")
	assert.Nil(t, err)
}

func TestBaseRepo_Exists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	repo := &BaseRepo{mockRegistry}
	ctx := context.Background()
	mockRegistry.EXPECT().Exists(gomock.Any(), "sample-key").Return(true, nil)
	ok, err := repo.Exists(ctx, "sample-key")
	assert.Nil(t, err)
	assert.Equal(t, ok, true)
}

func TestBaseRepo_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	repo := &BaseRepo{mockRegistry}
	ctx := context.Background()
	mockRegistry.EXPECT().Get(gomock.Any(), "sample-key").Return([]byte("{}"), nil)

	var model sampleModel
	err := repo.Get(ctx, "sample-key", &model)
	assert.Nil(t, err)
	assert.NotNil(t, model)
}

func TestBaseRepo_Delete(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	repo := &BaseRepo{mockRegistry}
	ctx := context.Background()
	mockRegistry.EXPECT().DeleteTree(gomock.Any(), "sample-key")
	err := repo.Delete(ctx, &sampleModel{})
	assert.Nil(t, err)
}

func TestBaseRepo_DeleteTree(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	repo := &BaseRepo{mockRegistry}
	ctx := context.Background()
	mockRegistry.EXPECT().DeleteTree(gomock.Any(), "sample")
	err := repo.DeleteTree(ctx, "sample")
	assert.Nil(t, err)
}

func TestBaseRepo_ListKeys(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	repo := &BaseRepo{mockRegistry}
	ctx := context.Background()
	mockRegistry.EXPECT().ListKeys(gomock.Any(), "sample").Return([]string{"sample-key"}, nil)
	keys, err := repo.ListKeys(ctx, "sample")
	assert.Nil(t, err)
	assert.Equal(t, keys, []string{"sample-key"})
}
