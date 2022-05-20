//go:build unit
// +build unit

package subscription

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/project"
	pCore "github.com/razorpay/metro/internal/project/mocks/core"
	repo "github.com/razorpay/metro/internal/subscription/mocks/repo"
	"github.com/razorpay/metro/internal/topic"
	tCore "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/stretchr/testify/assert"
)

func TestSubscriptionCore_CreateSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)

	sub := getSubModel()

	tpc := topic.Model{
		Name:               sub.GetSubscriptionTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedTopicProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(false, nil)
	mockTopicCore.EXPECT().Get(gomock.Any(), sub.Topic).Times(1).Return(&tpc, nil)
	mockTopicCore.EXPECT().CreateSubscriptionTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetSubscriptionTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateRetryTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetRetryTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName + topic.RetryTopicSuffix,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateDeadLetterTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetDeadLetterTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName + topic.DeadLetterTopicSuffix,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateTopic(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).Times(8) // number of delay topics being created
	mockRepo.EXPECT().Save(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	err := core.CreateSubscription(ctx, &sub)
	assert.Nil(t, err)
}

func TestSubscriptionCore_CreateSubscription_DLQTopic(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)
	sub := getSubModel()
	sub.Topic = "projects/project123/topics/subscription123-dlq"
	sub.ExtractedTopicName = "subscription123-dlq"
	tpc := topic.Model{
		Name:               sub.Topic,
		ExtractedTopicName: sub.ExtractedTopicName,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedTopicProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(false, nil)
	mockTopicCore.EXPECT().Get(gomock.Any(), sub.Topic).Times(1).Return(&tpc, nil)
	mockTopicCore.EXPECT().CreateSubscriptionTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetSubscriptionTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateRetryTopic(gomock.Any(), &topic.Model{
		Name:               sub.GetRetryTopic(),
		ExtractedTopicName: sub.ExtractedSubscriptionName + topic.RetryTopicSuffix,
		ExtractedProjectID: sub.ExtractedTopicProjectID,
		NumPartitions:      0,
	}).Times(1).Return(nil)
	mockTopicCore.EXPECT().CreateDeadLetterTopic(gomock.Any(), gomock.Any()).Times(0).Return(nil)
	mockTopicCore.EXPECT().CreateTopic(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).Times(8) // number of delay topics being created
	mockRepo.EXPECT().Save(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	err := core.CreateSubscription(ctx, &sub)
	assert.Nil(t, err)
}

func TestSubscriptionCore_UpdateSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)
	sub := getSubModel()

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(true, nil)
	mockRepo.EXPECT().Save(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	err := core.UpdateSubscription(ctx, &sub)
	assert.Nil(t, err)
}

func TestSubscriptionCore_UpdateSubscriptionNotExists(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)
	sub := getSubModel()

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
	mockRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).Times(1).Return(false, nil)

	err := core.UpdateSubscription(ctx, &sub)
	assert.NotNil(t, err)
}

func TestSubscriptionCore_UpdateSubscriptionProjectNotExists(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)

	core := NewCore(mockRepo, mockProjectCore, mockTopicCore)
	sub := getSubModel()

	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), sub.ExtractedSubscriptionProjectID).Times(1).Return(false, nil)

	err := core.UpdateSubscription(ctx, &sub)
	assert.NotNil(t, err)
}

func TestSubscriptionCore_CreateDelayTopics(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockTopicCore := tCore.NewMockICore(ctrl)

	err := createDelayTopics(ctx, nil, mockTopicCore)
	assert.Nil(t, err)

	err = createDelayTopics(ctx, &Model{}, nil)
	assert.Nil(t, err)

	err = createDelayTopics(ctx, nil, nil)
	assert.Nil(t, err)

	sub := &Model{
		Name:                           "projects/dummy1/subscriptions/subs1",
		Topic:                          "projects/dummy1/topics/topic1",
		ExtractedSubscriptionProjectID: "dummy1",
		ExtractedSubscriptionName:      "subs1",
	}

	mockTopicCore.EXPECT().CreateTopic(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).Times(8) // number of delay topics being created
	err = createDelayTopics(ctx, sub, mockTopicCore)
	assert.Nil(t, err)
}

func getSubModel() Model {
	sub := Model{
		Name:                           "projects/project123/subscriptions/subscription123",
		Topic:                          "projects/project123/topics/topic123",
		ExtractedSubscriptionProjectID: "project123",
		ExtractedTopicProjectID:        "project123",
		ExtractedSubscriptionName:      "subscription123",
		ExtractedTopicName:             "topic123",
		PushConfig: &PushConfig{
			PushEndpoint: "https://www.razorpay.com/api/v1",
		},
		AckDeadlineSeconds: 20,
		Labels:             map[string]string{},
	}
	return sub
}

func TestCore_DeleteSubscription(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		topicCore   topic.ICore
	}
	type args struct {
		ctx       context.Context
		m         *Model
		projectID string
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	sub := getSubModel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Delete Subscription successfully",
			fields: fields{
				repo:        mockRepo,
				projectCore: mockProjectCore,
				topicCore:   mockTopicCore,
			},
			args: args{
				ctx:       ctx,
				m:         &sub,
				projectID: sub.ExtractedSubscriptionProjectID,
			},
			wantErr: false,
		},
		{
			name: "Delete Subscription failed with error",
			fields: fields{
				repo:        mockRepo,
				projectCore: mockProjectCore,
				topicCore:   mockTopicCore,
			},
			args: args{
				ctx:       ctx,
				m:         &sub,
				projectID: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		c := &Core{
			repo:        tt.fields.repo,
			projectCore: tt.fields.projectCore,
			topicCore:   tt.fields.topicCore,
		}
		t.Run(tt.name, func(t *testing.T) {
			var err2 error = nil
			expectBool := true
			if len(tt.args.projectID) == 0 {
				err2 = fmt.Errorf("Invalid Project ID!")
				expectBool = false
				mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), tt.args.m.ExtractedSubscriptionProjectID).Times(1).Return(expectBool, err2)
			} else {
				mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), tt.args.m.ExtractedSubscriptionProjectID).Times(1).Return(true, nil)
				mockRepo.EXPECT().Exists(gomock.Any(), tt.args.m.Key()).Times(1).Return(true, nil)
				mockRepo.EXPECT().Delete(gomock.Any(), tt.args.m).Return(nil)
			}
			if err := c.DeleteSubscription(tt.args.ctx, tt.args.m); (err != nil) != tt.wantErr {
				t.Errorf("Core.DeleteSubscription() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCore_DeleteProjectSubscriptions(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		topicCore   topic.ICore
	}
	type args struct {
		ctx       context.Context
		projectID string
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	sub := getSubModel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Delete Project Subscription successfully",
			fields: fields{
				repo:        mockRepo,
				projectCore: mockProjectCore,
				topicCore:   mockTopicCore,
			},
			args: args{
				ctx:       ctx,
				projectID: sub.ExtractedSubscriptionProjectID,
			},
			wantErr: false,
		},
		{
			name: "Delete Project Subscription failed with errors",
			fields: fields{
				repo:        mockRepo,
				projectCore: mockProjectCore,
				topicCore:   mockTopicCore,
			},
			args: args{
				ctx:       ctx,
				projectID: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		c := &Core{
			repo:        tt.fields.repo,
			projectCore: tt.fields.projectCore,
			topicCore:   tt.fields.topicCore,
		}
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.args.projectID) != 0 {
				prefix := common.GetBasePrefix() + Prefix + tt.args.projectID
				mockRepo.EXPECT().DeleteTree(gomock.Any(), prefix).Return(nil)
			}
			if err := c.DeleteProjectSubscriptions(tt.args.ctx, tt.args.projectID); (err != nil) != tt.wantErr {
				t.Errorf("Core.DeleteProjectSubscriptions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCore_GetTopicFromSubscriptionName(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		topicCore   topic.ICore
	}
	type args struct {
		ctx              context.Context
		subscription     string
		projectID        string
		subscriptionName string
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)
	sub := getSubModel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Get Topic successfully",
			fields: fields{
				repo:        mockRepo,
				projectCore: mockProjectCore,
				topicCore:   mockTopicCore,
			},
			args: args{
				ctx:              ctx,
				subscription:     sub.Name,
				projectID:        sub.ExtractedSubscriptionProjectID,
				subscriptionName: sub.ExtractedSubscriptionName,
			},
			want:    sub.Topic,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		c := &Core{
			repo:        tt.fields.repo,
			projectCore: tt.fields.projectCore,
			topicCore:   tt.fields.topicCore,
		}
		t.Run(tt.name, func(t *testing.T) {
			prefix := common.GetBasePrefix() + Prefix + tt.args.projectID + "/" + tt.args.subscriptionName
			mockRepo.EXPECT().Get(gomock.Any(), prefix, &Model{}).Do(func(arg1 context.Context, arg2 string, mod *Model) {
				mod.Name = "projects/project123/subscriptions/subscription123"
				mod.Topic = "projects/project123/topics/topic123"
				mod.ExtractedSubscriptionProjectID = "project123"
				mod.ExtractedTopicProjectID = "project123"
				mod.ExtractedSubscriptionName = "subscription123"
				mod.ExtractedTopicName = "topic123"
				mod.PushConfig = &PushConfig{
					PushEndpoint: "https://www.razorpay.com/api/v1",
				}
				mod.AckDeadlineSeconds = 20
				mod.Labels = map[string]string{}

			}).Return(nil)
			got, err := c.GetTopicFromSubscriptionName(tt.args.ctx, tt.args.subscription)
			if (err != nil) != tt.wantErr {
				t.Errorf("Core.GetTopicFromSubscriptionName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Core.GetTopicFromSubscriptionName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCore_ListKeys(t *testing.T) {
	type fields struct {
		repo        IRepo
		projectCore project.ICore
		topicCore   topic.ICore
	}
	type args struct {
		ctx    context.Context
		prefix string
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockProjectCore := pCore.NewMockICore(ctrl)
	mockTopicCore := tCore.NewMockICore(ctrl)
	mockRepo := repo.NewMockIRepo(ctrl)

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "List Keys successfully",
			fields: fields{
				repo:        mockRepo,
				projectCore: mockProjectCore,
				topicCore:   mockTopicCore,
			},
			args: args{
				ctx:    ctx,
				prefix: "test-prefix",
			},
			want: []string{
				"key1", "key2",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		c := &Core{
			repo:        tt.fields.repo,
			projectCore: tt.fields.projectCore,
			topicCore:   tt.fields.topicCore,
		}
		t.Run(tt.name, func(t *testing.T) {
			expectedList := []string{
				"key1", "key2",
			}
			prefix := common.GetBasePrefix() + tt.args.prefix
			mockRepo.EXPECT().ListKeys(gomock.Any(), prefix).Return(expectedList, nil)
			got, err := c.ListKeys(tt.args.ctx, tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("Core.ListKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Core.ListKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}
