// +build unit

package interceptors

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/razorpay/metro/internal/credentials"
	mocks1 "github.com/razorpay/metro/internal/credentials/mocks/core"
	"github.com/razorpay/metro/pkg/encryption"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func Test_secureCompare(t *testing.T) {
	assert.True(t, secureCompare("correct", "correct"))
	assert.False(t, secureCompare("correct", "wrong"))
}

func Test_getUserPassword_Success(t *testing.T) {

	ctx := context.Background()
	// dummy12__13a011 2L9J4A0rdzcIO722089L
	pairs := []string{authorizationHeaderKey, "Basic ZHVtbXkxMl9fMTNhMDExOjJMOUo0QTByZHpjSU83MjIwODlM", "uri", "/v1/projects/dummy12/topics/t123"}
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)

	user, pwd, err := getUserPassword(newCtx)
	assert.Equal(t, "dummy12__13a011", user)
	assert.Equal(t, "2L9J4A0rdzcIO722089L", string(pwd))
	assert.Nil(t, err)
}

func Test_getUserPasswordProjectID_Failure1(t *testing.T) {
	_, _, err := getUserPassword(context.Background()) // empty metadata
	assert.NotNil(t, err)
}

func Test_getUserPassword_Failure2(t *testing.T) {
	ctx := context.Background()
	pairs := []string{authorizationHeaderKey, "", "uri", "/v1/projects/dummy12/topics/t123"}
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)

	_, _, err := getUserPassword(newCtx) // empty authorization header
	assert.NotNil(t, err)
}

func Test_getUserPasswordProjectID_Failure3(t *testing.T) {
	ctx := context.Background()
	pairs := []string{authorizationHeaderKey, "Basic ??<wrong-auth-header>", "uri", "/v1/projects/dummy12/topics/t123"}
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)

	_, _, err := getUserPassword(newCtx) // wrong authorization header
	assert.NotNil(t, err)
}

func Test_AdminAuth_Success(t *testing.T) {

	ctx := context.Background()
	pairs := []string{authorizationHeaderKey, "Basic YWRtaW46c3VwZXJzZWNyZXQ=", "uri", "/v1/projects/dummy12/topics/t123"}
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)

	ctx, err := AdminAuth(newCtx, &credentials.Model{
		Username: "admin",
		Password: "supersecret",
	})

	assert.Nil(t, err)
}

func Test_AdminAuth_Failure1(t *testing.T) {

	ctx := context.Background()
	pairs := []string{authorizationHeaderKey, "Basic YWRtaW46c3VwZXJzZWNyZXQ=", "uri", "/v1/projects/dummy12/topics/t123"}
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)

	ctx, err := AdminAuth(newCtx, &credentials.Model{
		Username: "admin",
		Password: "wrong-password",
	})

	assert.NotNil(t, err)
}
func Test_AdminAuth_Failure2(t *testing.T) {

	ctx := context.Background() // context without the authorization header
	ctx, err := AdminAuth(ctx, &credentials.Model{
		Username: "admin",
		Password: "wrong-password",
	})

	assert.NotNil(t, err)
}

func Test_AppAuth_Success(t *testing.T) {
	ctx := context.Background()
	pairs := []string{authorizationHeaderKey, "Basic ZHVtbXkxMl9fMTNhMDExOjJMOUo0QTByZHpjSU83MjIwODlM", "uri", "/v1/projects/dummy12/topics/t123"}
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)

	username := "dummy12__13a011"
	password := "2L9J4A0rdzcIO722089L"
	project := "dummy12"

	encryption.RegisterEncryptionKey("2K9HQKejNV0OkycszeuZ7e6QKwtbwrzO")
	dummyCreds := credentials.NewCredential(username, password)
	ctrl := gomock.NewController(t)
	mockCore := mocks1.NewMockICore(ctrl)
	mockCore.EXPECT().Get(gomock.Any(), project, username).Return(dummyCreds, nil)

	ctx, err := AppAuth(newCtx, mockCore, project)
	assert.Nil(t, err)
}

func Test_AppAuth_MissingHeader(t *testing.T) {
	ctx := context.Background()
	ctx, err := AppAuth(ctx, nil, "project-123") // missing auth header
	assert.NotNil(t, err)
}

func Test_AppAuth_WrongUsernameFormat(t *testing.T) {
	ctx := context.Background()
	pairs := []string{authorizationHeaderKey, "Basic YWRtaW46c3VwZXJzZWNyZXQ=", "uri", "/v1/projects/dummy12/topics/t123"} // wrong user name format
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)

	ctx, err := AppAuth(newCtx, nil, "project-123")
	assert.NotNil(t, err)
}

func Test_AppAuth_CredentialCoreError(t *testing.T) {
	ctx := context.Background()
	pairs := []string{authorizationHeaderKey, "Basic ZHVtbXkxMl9fMTNhMDExOjJMOUo0QTByZHpjSU83MjIwODlM", "uri", "/v1/projects/dummy12/topics/t123"}
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)
	projectID := "project-123"

	ctrl := gomock.NewController(t)
	mockCore := mocks1.NewMockICore(ctrl)
	mockCore.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("some error")) // credential core error

	ctx, err := AppAuth(newCtx, mockCore, projectID)
	assert.Equal(t, unauthenticatedError, err)
}

func Test_AppAuth_ProjectMismatch(t *testing.T) {
	ctx := context.Background()
	pairs := []string{authorizationHeaderKey, "Basic ZHVtbXkxMl9fMTNhMDExOjJMOUo0QTByZHpjSU83MjIwODlM", "uri", "/v1/projects/wrongProjectID/topics/t123"}
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)

	username := "dummy12__13a011"
	password := "2L9J4A0rdzcIO722089L"
	project := "dummy12"

	encryption.RegisterEncryptionKey("2K9HQKejNV0OkycszeuZ7e6QKwtbwrzO")
	dummyCreds := credentials.NewCredential(username, password)
	ctrl := gomock.NewController(t)
	mockCore := mocks1.NewMockICore(ctrl)
	mockCore.EXPECT().Get(gomock.Any(), project, username).Return(dummyCreds, nil)

	// resource projectID and credential projectID mismatch
	ctx, err := AppAuth(newCtx, mockCore, "wrong-project")
	assert.Equal(t, unauthorizedError, err)
}

func Test_AppAuth_WrongPassword(t *testing.T) {
	ctx := context.Background()
	pairs := []string{authorizationHeaderKey, "Basic ZHVtbXkxMl9fMTNhMDExOjJMOUo0QTByZHpjSU83MjIwODlM", "uri", "/v1/projects/dummy12/topics/t123"}
	md := metadata.Pairs(pairs...)
	newCtx := metadata.NewIncomingContext(ctx, md)

	username := "dummy12__13a011"
	password := "password-mismatch" // wrong password sent in authorizationHeaderKey
	project := "dummy12"

	encryption.RegisterEncryptionKey("2K9HQKejNV0OkycszeuZ7e6QKwtbwrzO")
	dummyCreds := credentials.NewCredential(username, password)
	ctrl := gomock.NewController(t)
	mockCore := mocks1.NewMockICore(ctrl)
	mockCore.EXPECT().Get(gomock.Any(), project, username).Return(dummyCreds, nil)

	ctx, err := AppAuth(newCtx, mockCore, project)
	assert.Equal(t, unauthenticatedError, err)
}
