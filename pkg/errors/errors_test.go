package errors_test

import (
	goErr "errors"
	"testing"

	"github.com/razorpay/metro/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var (
	defaultClass = errors.NewClass("default", "default_code")
	testClass    = errors.NewClass("test", "test_code")
)

func TestError_New(t *testing.T) {
	tests := []struct {
		name     string
		err      errors.IError
		validate func(err errors.IError)
	}{
		{
			name: "basic error without code",
			err:  testClass.New(""),
			validate: func(err errors.IError) {
				assert.Equal(t, "test_code", err.Internal().Code())
				assert.Equal(t, "test", err.Class().Name())
				assert.Equal(t, &errors.Public{
					Code:        "server_error",
					Description: "something bad happened",
					Metadata:    map[string]string{},
				}, err.Public())
				assert.Equal(t, "test_code", err.Internal().Error())
				assert.Equal(t, "test", testClass.Error())
			},
		},
		{
			name: "basic error with unmapped public detail",
			err:  defaultClass.New("default_error_code"),
			validate: func(err errors.IError) {
				assert.Equal(t, "default_error_code", err.Internal().Code())
				assert.Equal(t, "default", err.Class().Name())
				assert.Equal(t, &errors.Public{
					Code:        "server_error",
					Description: "something bad happened",
					Metadata:    map[string]string{},
				}, err.Public())
				assert.Equal(t, "default_error_code", err.Internal().Error())
			},
		},
		{
			name: "basic error with mapped public error",
			err:  defaultClass.New(errors.BadRequestError),
			validate: func(err errors.IError) {
				assert.Equal(t, errors.BadRequestError, err.Internal().Code())
				assert.Equal(t, "default", err.Class().Name())
				assert.Equal(t, &errors.Public{
					Code:        "BAD_REQUEST_ERROR",
					Description: "Bad request",
					Metadata:    map[string]string{},
				}, err.Public())
				assert.Equal(t, "default: bad_request_error", err.Error())
			},
		},
		{
			name: "with public details",
			err: defaultClass.New(errors.BadRequestError).
				WithPublic(&errors.Public{
					Code:        "custom public code",
					Description: "this is a custom public error",
					Field:       "some field",
					Source:      "customer",
					Step:        "payment_authentication",
					Reason:      "invalid_otp",
					Metadata: map[string]string{
						"key": "value",
					},
				}),
			validate: func(err errors.IError) {
				assert.Equal(t, errors.BadRequestError, err.Internal().Code())
				assert.Equal(t, "default", err.Class().Name())
				assert.Equal(t, &errors.Public{
					Code:        "custom public code",
					Description: "this is a custom public error",
					Field:       "some field",
					Source:      "customer",
					Step:        "payment_authentication",
					Reason:      "invalid_otp",
					Metadata: map[string]string{
						"key": "value",
					},
				}, err.Public())
			},
		},
		{
			name: "with internal details",
			err: defaultClass.New("default_error_code").
				Wrap(goErr.New("some error")).
				WithInternalMetadata(map[string]string{
					"key": "value",
				}),
			validate: func(err errors.IError) {
				assert.Equal(t, "default_error_code", err.Internal().Code())
				assert.Equal(t, "default", err.Class().Name())
				assert.Equal(t, goErr.New("some error"), err.Unwrap())
				assert.Equal(t, map[string]string{
					"key": "value",
				}, err.Internal().Metadata())
				assert.Equal(t, "some error", err.Internal().Error())
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.validate(test.err)
		})
	}
}

func TestError_Serialize(t *testing.T) {
	tests := []struct {
		name         string
		err          errors.IError
		withInternal bool
		response     string
	}{
		{
			name:         "serialize with internal",
			err:          defaultClass.New(errors.Default),
			withInternal: true,
			response: "{\"error\":{\"code\":\"server_error\",\"description\":\"something bad happened\"," +
				"\"step\":\"\",\"reason\":\"\",\"field\":\"\",\"source\":\"\",\"metadata\":{}}," +
				"\"internal_error\":{\"code\":\"default\",\"description\":\"default\"}}",
		},
		{
			name:         "serialize without internal",
			err:          defaultClass.New(errors.Default),
			withInternal: false,
			response: "{\"error\":{\"code\":\"server_error\",\"description\":\"something bad happened\"," +
				"\"step\":\"\",\"reason\":\"\",\"field\":\"\",\"source\":\"\",\"metadata\":{}}}",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			b, err := test.err.Serialize(test.withInternal)
			assert.Nil(t, err)
			assert.Equal(t, test.response, string(b))
		})
	}
}

func TestStack_StackTrace(t *testing.T) {
	err := defaultClass.New("default_error_code")

	trace := err.Internal().StackTrace()

	assert.Equal(t, 3, len(trace))
	assert.Regexp(t,
		`^.+pkg/errors_test.TestStack_StackTrace .+/pkg/errors/errors_test.go:\d+`,
		trace[0].String())
}

func TestError_Register(t *testing.T) {
	public := errors.Public{
		Code:        "custom_public_code",
		Description: "custom description",
		Metadata: map[string]string{
			"key": "value",
		},
	}

	errors.Register("custom_code", &public)
	err := defaultClass.New("custom_code")

	assert.Equal(t, &public, err.Public())
}

func TestError_Is(t *testing.T) {
	tests := []struct {
		name  string
		err1  error
		err2  error
		match bool
	}{
		{
			name:  "different errors",
			err1:  defaultClass.New(errors.Default),
			err2:  testClass.New(errors.Default),
			match: false,
		},
		{
			name:  "same errors",
			err1:  defaultClass.New(errors.Default),
			err2:  defaultClass.New(errors.BadRequestError),
			match: true,
		},
		{
			name:  "wrapped errors",
			err1:  testClass.New(errors.BadRequestError).Wrap(defaultClass.New("some_code")),
			err2:  defaultClass.New(errors.Default),
			match: true,
		},
		{
			name:  "wrapped invalid errors",
			err1:  testClass.New(errors.BadRequestError),
			err2:  goErr.New("some error"),
			match: false,
		},
		{
			name:  "is of class",
			err1:  testClass.New(errors.BadRequestError).Wrap(defaultClass.New("some_code")),
			err2:  defaultClass,
			match: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			res := goErr.Is(test.err1, test.err2)
			assert.Equal(t, test.match, res)
		})
	}
}
