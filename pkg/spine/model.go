package spine

import (
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/gobuffalo/nulls"
	"github.com/gofrs/uuid"
	"github.com/jinzhu/gorm"

	"github.com/razorpay/metro/pkg/errors"
	"github.com/razorpay/metro/pkg/spine/datatype"
)

const (
	AttributeID        = "id"
	AttributeCreatedAt = "created_at"
	AttributeUpdatedAt = "updated_at"
	AttributeDeletedAt = "deleted_at"
)

type Model struct {
	ID        string `json:"id"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type IModel interface {
	TableName() string
	EntityName() string
	GetID() string
	Validate() errors.IError
	SetDefaults() errors.IError
}

// Validate validates base Model.
func (m *Model) Validate() errors.IError {
	return GetValidationError(
		validation.ValidateStruct(
			m,
			validation.Field(&m.ID, validation.By(datatype.IsRZPID)),
			validation.Field(&m.CreatedAt, validation.By(datatype.IsTimestamp)),
			validation.Field(&m.UpdatedAt, validation.By(datatype.IsTimestamp)),
		),
	)
}

// GetID gets identifier of entity.
func (m *Model) GetID() string {
	return m.ID
}

// GetCreatedAt gets created time of entity
func (m *Model) GetCreatedAt() int64 {
	return m.CreatedAt
}

// GetUpdatedAt gets last updated time of entity
func (m *Model) GetUpdatedAt() int64 {
	return m.UpdatedAt
}

// BeforeCreate sets new Razorpay id for the settlement.
func (m *Model) BeforeCreate(scope *gorm.Scope) {
	idField, _ := scope.FieldByName("ID")
	if idField.IsBlank {
		v, _ := uuid.NewV1()
		_ = idField.Set(v.String())
	}
}

// SoftDeletableModel struct is base for entity models
// it extends the capability of having basic attributes
// with soft delete option
type SoftDeletableModel struct {
	Model
	DeletedAt nulls.Int64 `sql:"DEFAULT:null" json:"deleted_at"`
}

// Validate validates SoftDeletableOwnedModel.
func (sm *SoftDeletableModel) Validate() errors.IError {
	err := GetValidationError(
		validation.ValidateStruct(
			sm,
			validation.Field(&sm.DeletedAt,
				validation.By(datatype.ValidateNullableInt64(sm.DeletedAt, datatype.IsTimestamp))),
		),
	)

	if err == nil {
		return sm.Model.Validate()
	}

	return err
}
