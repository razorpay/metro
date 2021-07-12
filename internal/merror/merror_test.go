package merror

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_New(t *testing.T) {
	merr := New(NotFound, "some error occured")
	grpcErr := merr.ToGRPCError()
	assert.Equal(t, "some error occured", merr.Error())
	assert.Equal(t, "rpc error: code = NotFound desc = some error occured", grpcErr.Error())
}

func Test_Newf(t *testing.T) {
	merr := Newf(Unauthenticated, "some error occured %v", "here")
	grpcErr := merr.ToGRPCError()
	assert.Equal(t, "some error occured here", merr.Error())
	assert.Equal(t, "rpc error: code = Unauthenticated desc = some error occured here", grpcErr.Error())
}
