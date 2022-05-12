//go:build unit
// +build unit

package project

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	mocks "github.com/razorpay/metro/internal/project/mocks/repo"
	"github.com/stretchr/testify/assert"
)

func TestProject_NewCore(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	c := NewCore(mockRepo)
	assert.NotNil(t, c)
}

func TestCore_CreateProject(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	project := getDummyProjectModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(gomock.Any(), project.Key())
	mockRepo.EXPECT().Save(gomock.Any(), project)
	err := core.CreateProject(ctx, project)
	assert.NoError(t, err)
}

func TestCore_Exists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	project := getDummyProjectModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(gomock.Any(), project.Key()).Return(true, nil)
	ok, err := core.Exists(ctx, project.Key())
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestCore_ExistsWithID(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	project := getDummyProjectModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(gomock.Any(), project.Key()).Return(true, nil)
	ok, err := core.ExistsWithID(ctx, project.ProjectID)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestCore_Get(t *testing.T) {
	type fields struct {
		repo IRepo
	}
	type args struct {
		ctx       context.Context
		projectID string
	}
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	project := getDummyProjectModel()
	ctx := context.Background()

	testModel := &Model{}
	testModel.SetVersion("123")

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Model
		wantErr bool
	}{
		{
			name: "Test1",
			fields: fields{
				repo: mockRepo,
			},
			args: args{
				ctx:       ctx,
				projectID: project.ProjectID,
			},
			want:    testModel,
			wantErr: false,
		},
		{
			name: "Test2",
			fields: fields{
				repo: mockRepo,
			},
			args: args{
				ctx:       ctx,
				projectID: "",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Core{
				repo: tt.fields.repo,
			}
			var err2 error = nil
			if len(tt.args.projectID) == 0 {
				err2 = fmt.Errorf("Invalid Project ID!")
			}
			mod := &Model{}
			mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), mod).Do(func(arg1 context.Context, arg2 string, mod *Model) {
				if err2 == nil {
					mod.SetVersion("123")
				}
			}).Return(err2)
			got, err := c.Get(tt.args.ctx, tt.args.projectID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Core.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Core.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCore_DeleteProject(t *testing.T) {
	type fields struct {
		repo IRepo
	}
	type args struct {
		ctx context.Context
		m   *Model
	}
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	project := getDummyProjectModel()
	ctx := context.Background()

	dummyModel := &Model{
		Name:      "test",
		ProjectID: "",
		Labels:    map[string]string{"label": "value"},
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				repo: mockRepo,
			},
			args: args{
				ctx: ctx,
				m:   project,
			},
			wantErr: false,
		},
		{
			name: "test2",
			fields: fields{
				repo: mockRepo,
			},
			args: args{
				ctx: ctx,
				m:   dummyModel,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Core{
				repo: tt.fields.repo,
			}
			var err2 error = nil
			if len(tt.args.m.ProjectID) == 0 {
				err2 = fmt.Errorf("Invalid Project ID!")
				mockRepo.EXPECT().Exists(gomock.Any(), tt.args.m.Key()).Return(false, err2)
			} else {
				mockRepo.EXPECT().Exists(gomock.Any(), tt.args.m.Key()).Return(true, err2)
				mockRepo.EXPECT().Delete(gomock.Any(), tt.args.m).Return(err2)
			}
			if err := c.DeleteProject(tt.args.ctx, tt.args.m); (err != nil) != tt.wantErr {
				t.Errorf("Core.DeleteProject() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
