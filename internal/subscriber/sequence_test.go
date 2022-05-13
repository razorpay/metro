package subscriber

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	mocks "github.com/razorpay/metro/internal/node/mocks/repo"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/stretchr/testify/assert"
)

const (
	orderingKey string = "test-key"
)

func Test_offsetSequenceManager_GetLastMessageSequenceNum(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)

	tests := []struct {
		expected int32
		wantErr  bool
		err      error
	}{
		{
			expected: int32(0),
			wantErr:  false,
			err:      nil,
		}, {
			expected: 0,
			wantErr:  true,
			err:      fmt.Errorf("Something went wrong"),
		},
	}

	for _, tt := range tests {
		mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(arg context.Context, arg2 string, arg3 *offset.Model) interface{} {
				arg3.LatestOffset = tt.expected
				return tt.err
			})
		offsetSeqManager := getMockSequenceManager(ctx, mockRepo)
		got, err := offsetSeqManager.GetLastMessageSequenceNum(ctx, getMockSubModel(), partition, orderingKey)
		assert.Equal(t, tt.wantErr, err != nil)
		assert.Equal(t, tt.expected, got)
	}
}

func Test_offsetSequenceManager_SetOrderedSequenceNum(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	sequenceNum := int32(20)

	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Return(true, nil)
	mockRepo.EXPECT().Save(gomock.Any(), gomock.Any()).Return(nil)
	mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(arg context.Context, arg2 string, arg3 *offset.Model) interface{} {
			arg3.LatestOffset = sequenceNum
			return nil
		}).AnyTimes()

	offsetSeqManager := getMockSequenceManager(ctx, mockRepo)
	err := offsetSeqManager.SetOrderedSequenceNum(ctx, getMockSubModel(), partition, orderingKey, sequenceNum)
	assert.NoError(t, err)
	got, err := offsetSeqManager.GetLastMessageSequenceNum(ctx, getMockSubModel(), partition, orderingKey)
	assert.NoError(t, err)
	assert.Equal(t, sequenceNum, got)
}

func Test_offsetSequenceManager_GetLastSequenceStatus(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	tests := []struct {
		exists   bool
		expected *lastSequenceStatus
		err      error
		wantErr  bool
	}{
		{
			exists:   false,
			expected: nil,
			err:      nil,
			wantErr:  false,
		},
		{
			exists: true,
			expected: &lastSequenceStatus{
				SequenceNum: 10,
				Status:      sequenceSuccess,
			},
			err:     nil,
			wantErr: false,
		},
		{
			exists:   true,
			expected: nil,
			err:      fmt.Errorf("Something went wrong"),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Return(tt.exists, nil)
		mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(arg context.Context, arg2 string, arg3 *offset.Status) interface{} {
				if tt.expected != nil {
					arg3.LatestOffset = tt.expected.SequenceNum
					arg3.OffsetStatus = string(tt.expected.Status)
				}
				return tt.err
			}).AnyTimes()
		offsetSeqManager := getMockSequenceManager(ctx, mockRepo)
		got, err := offsetSeqManager.GetLastSequenceStatus(ctx, getMockSubModel(), partition, orderingKey)
		assert.Equal(t, tt.wantErr, err != nil)
		if !reflect.DeepEqual(got, tt.expected) {
			t.Errorf("Get Status() = %v, want %v", got, tt.expected)
		}
	}
}

func Test_offsetSequenceManager_SetLastSequenceStatus(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	expected := &lastSequenceStatus{
		SequenceNum: 10,
		Status:      sequenceSuccess,
	}

	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Return(true, nil)
	mockRepo.EXPECT().Save(gomock.Any(), gomock.Any()).Return(nil)
	mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(arg context.Context, arg2 string, arg3 *offset.Status) interface{} {
			arg3.LatestOffset = expected.SequenceNum
			arg3.OffsetStatus = string(expected.Status)
			return nil
		}).AnyTimes()

	offsetSeqManager := getMockSequenceManager(ctx, mockRepo)
	err := offsetSeqManager.SetLastSequenceStatus(ctx, getMockSubModel(), partition, orderingKey, expected)
	assert.NoError(t, err)
	got, err := offsetSeqManager.GetLastSequenceStatus(ctx, getMockSubModel(), partition, orderingKey)
	assert.NoError(t, err)
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("Get Status() = %v, want %v", got, expected)
	}
}

func Test_offsetSequenceManager_DeleteSequence(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	mockRepo.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil).AnyTimes().AnyTimes()

	offsetSeqManager := getMockSequenceManager(ctx, mockRepo)
	err := offsetSeqManager.DeleteSequence(ctx, getMockSubModel(), partition, orderingKey)
	assert.NoError(t, err)
}

func getMockSequenceManager(ctx context.Context, mockRepo offset.IRepo) OrderingSequenceManager {
	core := offset.NewCore(mockRepo)
	return NewOffsetSequenceManager(ctx, core)
}

func getMockSubModel() *subscription.Model {
	return &subscription.Model{
		Topic: topic,
		Name:  subName,
	}
}
