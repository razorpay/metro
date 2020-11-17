package errorclass

import (
	"github.com/razorpay/metro/pkg/errors"
)

var (
	ErrValidationFailure = errors.NewClass("validation_failure", errors.BadRequestValidationFailureException)
)
