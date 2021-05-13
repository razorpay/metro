// +build unit

package common

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

func TestBaseRepo_Create(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	repo := &BaseRepo{mockRegistry}
	ctx := context.Background()
	mockRegistry.EXPECT().Put("sample-key", []byte("{}"))
	err := repo.Create(ctx, &sampleModel{})
	assert.Nil(t, err)
}

type sampleModel struct{}

func (s *sampleModel) Key() string { return "sample-key" }

func (s *sampleModel) Prefix() string { return "sample-prefix" }
