//go:build unit
// +build unit

package project

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
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
	ctx := context.Background()

	project1 := &Model{
		Name:      "test-1",
		ProjectID: "testID-1",
		Labels:    map[string]string{"label": "value"},
	}
	project2 := &Model{
		Name:      "test-2",
		ProjectID: "testID-2",
		Labels:    map[string]string{"label": "value"},
	}
	project3 := &Model{
		Name:      "test-3",
		ProjectID: "testID-3",
		Labels:    map[string]string{"label": "value"},
	}
	project1.SetVersion("1")
	project2.SetVersion("2")
	project3.SetVersion("3")

	num := rand.Intn(3)

	testProject := &Model{}
	projectMap := map[int]*Model{
		1: project1,
		2: project2,
		3: project3,
	}
	testProject = projectMap[num]

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Model
		wantErr bool
	}{
		{
			name: "Get Project Successfully.",
			fields: fields{
				repo: mockRepo,
			},
			args: args{
				ctx:       ctx,
				projectID: testProject.ProjectID,
			},
			want:    testProject,
			wantErr: false,
		},
		{
			name: "Throw error because of Invalid Project ID.",
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
	c := &Core{
		repo: mockRepo,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err2 error = nil
			if len(tt.args.projectID) == 0 {
				err2 = fmt.Errorf("Invalid Project ID!")
			}
			mockRepo.EXPECT().Get(gomock.Any(), gomock.Any(), &Model{}).Do(func(arg1 context.Context, arg2 string, mod *Model) {
				if err2 == nil {
					id := strings.Split(tt.args.projectID, "-")
					serial := id[1]

					mod.ProjectID = "testID" + "-" + serial
					mod.Name = "test" + "-" + serial
					mod.Labels = map[string]string{"label": "value"}
					mod.SetVersion(serial)
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
			name: "Delete existing project with No errors.",
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
			name: "Get error as Project doesn't exist.",
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
	c := &Core{
		repo: mockRepo,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err2 error = nil
			if len(tt.args.m.ProjectID) == 0 {
				err2 = fmt.Errorf("Invalid Project ID!")
				mockRepo.EXPECT().Exists(gomock.Any(), tt.args.m.Key()).Return(false, err2)
			} else {
				mockRepo.EXPECT().Exists(gomock.Any(), tt.args.m.Key()).Return(true, nil)
				mockRepo.EXPECT().Delete(gomock.Any(), tt.args.m).Return(nil)
			}
			if err := c.DeleteProject(tt.args.ctx, tt.args.m); (err != nil) != tt.wantErr {
				t.Errorf("Core.DeleteProject() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
