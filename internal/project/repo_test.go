// +build unit

package project

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

func TestRepo_NewRepo(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	r := NewRepo(mockRegistry)
	assert.NotNil(t, r)
}
