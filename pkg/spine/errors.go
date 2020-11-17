package spine

import (
	"github.com/jinzhu/gorm"
	"github.com/lib/pq"

	"github.com/razorpay/metro/pkg/errors"
)

const (
	PQCodeUniqueViolation = "unique_violation"

	errDBError                   = "db_error"
	errNoRowAffected             = "no_row_affected"
	errRecordNotFound            = "record_not_found"
	errValidationFailure         = "validation_failure"
	errUniqueConstraintViolation = "unique_constraint_violation"
)

var (
	DBError                   = errors.NewClass(errDBError, errDBError)
	NoRowAffected             = errors.NewClass(errNoRowAffected, errNoRowAffected)
	RecordNotFound            = errors.NewClass(errRecordNotFound, errRecordNotFound)
	ValidationFailure         = errors.NewClass(errValidationFailure, errValidationFailure)
	UniqueConstraintViolation = errors.NewClass(errUniqueConstraintViolation, errUniqueConstraintViolation)
)

// GetDBError accepts db instance and the details
// creates appropriate error based on the type of query result
// if there is no error then returns nil
func GetDBError(d *gorm.DB) errors.IError {
	if d.Error == nil {
		return nil
	}

	// check of error is specific to dialect
	if de, ok := DialectError(d); ok {
		// is the specific error is captured then return it
		// else try construct further errors
		if err := de.ConstructError(); err != nil {
			return err
		}
	}

	// Construct error based on type of db operation
	err := func() errors.IError {
		switch true {
		case d.RecordNotFound():
			return RecordNotFound.New(errRecordNotFound)

		default:
			return DBError.New(errDBError)
		}
	}()

	// add specific details of error
	return err.Wrap(d.Error)
}

// GetValidationError wraps the error and returns instance of ValidationError
// if the provided error is nil then it just returns nil
func GetValidationError(err error) errors.IError {
	if err != nil {
		return ValidationFailure.
			New(errValidationFailure).
			Wrap(err)
	}

	return nil
}

// DialectError returns true if the error is from dialect
func DialectError(d *gorm.DB) (IDialectError, bool) {
	switch d.Error.(type) {
	case *pq.Error:
		return pqError{d.Error.(*pq.Error)}, true
	default:
		return nil, false
	}
}

// IDialectError interface to handler dialect related errors
type IDialectError interface {
	ConstructError() errors.IError
}

// pqError holds the error occurred by postgres
type pqError struct {
	err *pq.Error
}

// ConstructError will create appropriate error based on dialect
func (pqe pqError) ConstructError() errors.IError {
	switch pqe.err.Code.Name() {
	case PQCodeUniqueViolation:
		return UniqueConstraintViolation.
			New(errUniqueConstraintViolation).
			Wrap(pqe.err)

	default:
		return nil
	}
}
