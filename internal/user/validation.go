package user

import (
	validation "github.com/go-ozzo/ozzo-validation/v4"

	"github.com/razorpay/metro/internal/errorclass"
	"github.com/razorpay/metro/pkg/errors"
)

func (u *User) Validate() errors.IError {
	err := validation.ValidateStruct(u,
		// id, required, length 14
		validation.Field(&u.ID, validation.Required, validation.RuneLength(14, 14)),

		// first_name, required, string, length 1-30
		validation.Field(&u.FirstName, validation.Required, validation.RuneLength(1, 30)),

		// first_name, required, string, length 1-30
		validation.Field(&u.LastName, validation.Required, validation.RuneLength(1, 30)),

		// status, required, string
		validation.Field(&u.Status, validation.Required, validation.In("created", "addmore")),
	)

	if err == nil {
		return nil
	}

	publicErr := errorclass.ErrValidationFailure.New("").
		Wrap(err).
		WithPublic(&errors.Public{
			Description: err.Error(),
		})

	return publicErr
}

func (cp *CreateParams) Validate() errors.IError {
	err := validation.ValidateStruct(cp,
		// first_name, required, string, length 1-30
		validation.Field(&cp.FirstName, validation.Required, validation.RuneLength(1, 30)),

		// last_name, required, string, length 1-30
		validation.Field(&cp.LastName, validation.Required, validation.RuneLength(1, 30)),
	)

	if err == nil {
		return nil
	}

	publicErr := errorclass.ErrValidationFailure.New("").
		Wrap(err).
		WithPublic(&errors.Public{
			Description: err.Error(),
		})

	return publicErr
}
