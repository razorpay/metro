// +build unit

package credentials

import (
	"context"
	"testing"

	"github.com/razorpay/metro/internal/common"

	"github.com/golang/mock/gomock"
	mocks1 "github.com/razorpay/metro/internal/credentials/mocks/repo"
	mocks2 "github.com/razorpay/metro/internal/project/mocks/core"
	"github.com/stretchr/testify/assert"
)

func TestCredential_NewCore(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks1.NewMockIRepo(ctrl)
	mockCore := mocks2.NewMockICore(ctrl)
	c := NewCore(mockRepo, mockCore)
	assert.NotNil(t, c)
}

func TestCore_Create(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks1.NewMockIRepo(ctrl)
	mockCore := mocks2.NewMockICore(ctrl)
	c := NewCore(mockRepo, mockCore)
	ctx := context.Background()

	m := getDummyCredentials()
	mockCore.EXPECT().ExistsWithID(ctx, m.ProjectID).Return(true, nil)
	mockRepo.EXPECT().Exists(ctx, m.Key()).Return(false, nil)
	mockRepo.EXPECT().Save(ctx, m).Return(nil)
	err := c.Create(ctx, m)
	assert.NoError(t, err)
}

func TestCore_Delete(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks1.NewMockIRepo(ctrl)
	mockCore := mocks2.NewMockICore(ctrl)
	c := NewCore(mockRepo, mockCore)
	ctx := context.Background()

	m := getDummyCredentials()

	mockRepo.EXPECT().Delete(ctx, m).Return(nil)
	err := c.Delete(ctx, m)
	assert.NoError(t, err)
}

func TestCore_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks1.NewMockIRepo(ctrl)
	mockCore := mocks2.NewMockICore(ctrl)
	c := NewCore(mockRepo, mockCore)
	ctx := context.Background()

	project := "project123"
	username := "project123__80d643"
	prefix := common.GetBasePrefix() + Prefix + project + "/" + username
	m := &Model{}
	mockRepo.EXPECT().Get(ctx, prefix, m).Return(nil)
	_, err := c.Get(ctx, project, username)
	assert.NoError(t, err)
}
