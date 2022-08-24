//go:build unit
// +build unit

package node

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/common"
	mocks "github.com/razorpay/metro/internal/node/mocks/repo"
	"github.com/stretchr/testify/assert"
)

func TestNode_NewCore(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	c := NewCore(mockRepo)
	assert.NotNil(t, c)
}

func TestCore_CreateNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	node := getDummyNodeModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, node.Key())
	mockRepo.EXPECT().Save(ctx, node)
	err := core.CreateNode(ctx, node)
	assert.NoError(t, err)
}

func TestCore_AcquireNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	node := getDummyNodeModel()
	ctx := context.Background()
	mockRepo.EXPECT().Acquire(ctx, node, "id").Return(nil)
	err := core.AcquireNode(ctx, node, "id")
	assert.NoError(t, err)
}

func TestCore_Exists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	node := getDummyNodeModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, node.Key()).Return(true, nil)
	ok, err := core.Exists(ctx, node.Key())
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestCore_ExistsWithID(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	node := getDummyNodeModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, node.Key()).Return(true, nil)
	ok, err := core.ExistsWithID(ctx, node.ID)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestCore_List(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	ctx := context.Background()
	node := getDummyNodeModel()

	tests := []struct {
		input   []common.IModel
		prefix  string
		want    []*Model
		wantErr bool
	}{
		{
			input:   []common.IModel{node},
			prefix:  "nodes/",
			want:    []*Model{node},
			wantErr: false,
		},
		{
			input:   []common.IModel{},
			prefix:  "test-prefix/",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		var err error
		if tt.wantErr {
			err = fmt.Errorf("Something went wrong")
		}
		mockRepo.EXPECT().List(ctx, common.GetBasePrefix()+tt.prefix).Return(tt.input, err)
		got, err := core.List(ctx, tt.prefix)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("List got %v, want %v", got, tt.want)
		}
		assert.Equal(t, tt.wantErr, err != nil)
	}
}

func TestCore_ListKeys(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	ctx := context.Background()

	tests := []struct {
		input   []string
		prefix  string
		want    []string
		wantErr bool
	}{
		{
			input:   []string{"1", "2"},
			prefix:  "nodes/",
			want:    []string{"1", "2"},
			wantErr: false,
		},
		{
			input:   []string{},
			prefix:  "test-prefix/",
			want:    []string{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		var err error
		if tt.wantErr {
			err = fmt.Errorf("Something went wrong")
		}
		mockRepo.EXPECT().ListKeys(ctx, common.GetBasePrefix()+tt.prefix).Return(tt.input, err)
		got, err := core.ListKeys(ctx, tt.prefix)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("ListKeys got %v, want %v", got, tt.want)
		}
		assert.Equal(t, tt.wantErr, err != nil)
	}
}

func TestCore_DeleteNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	node := getDummyNodeModel()
	ctx := context.Background()

	tests := []struct {
		exists  bool
		wantErr bool
	}{
		{
			exists:  true,
			wantErr: false,
		},
		{
			exists:  false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		var err error
		if tt.wantErr {
			err = fmt.Errorf("Something went wrong")
		}
		mockRepo.EXPECT().Exists(ctx, node.Key()).Return(tt.exists, nil)
		mockRepo.EXPECT().Delete(ctx, node).AnyTimes()

		err = core.DeleteNode(ctx, node)
		assert.Equal(t, tt.wantErr, err != nil)
	}
}
