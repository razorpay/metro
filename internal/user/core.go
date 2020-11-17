package user

import (
	"context"
	goerr "errors"
	"github.com/gofrs/uuid"

	"github.com/razorpay/metro/internal/job"

	"github.com/fatih/structs"
	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/pkg/errors"
)

type ICore interface {
	Create(ctx context.Context, params *CreateParams) (*User, errors.IError)
	Find(ctx context.Context, id string) (*User, errors.IError)
	FindMany(ctx context.Context, params IFindManyParams) (*[]User, errors.IError)
	Approve(ctx context.Context, id string) (*User, errors.IError)
	Delete(ctx context.Context, id string) errors.IError
	Activate(ctx context.Context, id string) errors.IError
}

type Core struct {
	repo IRepo
}

// NewCore returns a new instance of *Core
func NewCore(ctx context.Context, repo IRepo) *Core {
	_ = ctx
	return &Core{repo: repo}
}

// CreateParams has attributes that are required for user.Create()
type CreateParams struct {
	FirstName string
	LastName  string
}

// Create creates a user
func (c *Core) Create(ctx context.Context, params *CreateParams) (*User, errors.IError) {
	span, _ := ot.StartSpanFromContext(ctx, "user.core.Create")
	defer span.Finish()

	boot.Logger(ctx).Infow("user create request", map[string]interface{}{"params": params})
	var err errors.IError

	spanValidate, _ := ot.StartSpanFromContext(ctx, "user.core.Validate")
	err = params.Validate()
	if err != nil {
		spanValidate.SetTag("error", true)
		span.LogFields(otlog.String("event", "error"), otlog.String("message", err.Error()))
		spanValidate.Finish()
		boot.Logger(ctx).WithError(err).Error("user create validation failed")
		return nil, err
	}
	spanValidate.Finish()

	userId, _ := uuid.NewV1()

	// Create a new model
	user := User{}
	user.ID = userId.String()
	user.FirstName = params.FirstName
	user.LastName = params.LastName
	user.Status = Created

	err = c.repo.Create(ctx, &user)
	if err != nil {
		boot.Logger(ctx).WithError(err).Errorw("user create failed", map[string]interface{}{"user_id": user.ID})
		return nil, err
	}

	boot.Logger(ctx).Infow("user created", map[string]interface{}{"user_id": user.ID})

	return &user, nil
}

func (c *Core) Find(ctx context.Context, id string) (*User, errors.IError) {
	user := User{}

	err := c.repo.FindByID(ctx, &user, id)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

type IFindManyParams interface {
	GetCount() int32
	GetSkip() int32
	GetFrom() int32
	GetTo() int32

	// custom
	GetFirstName() string
	GetLastName() string
}

type FindManyParams struct {
	// pagination
	Count int32
	Skip  int32
	From  int32
	To    int32

	// custom
	FirstName string
	LastName  string
}

func (c *Core) FindMany(ctx context.Context, params IFindManyParams) (*[]User, errors.IError) {
	var users []User

	conditionStr := structs.New(params)
	// use the json tag name, so we can respect omitempty tags
	conditionStr.TagName = "json"
	conditions := conditionStr.Map()

	err := c.repo.FindMany(ctx, &users, conditions)
	if err != nil {
		return nil, err
	}

	return &users, nil
}

func (c *Core) Approve(ctx context.Context, id string) (*User, errors.IError) {
	// todo: mutex?

	boot.Logger(ctx).Infow("user approve request", map[string]interface{}{"user_id": id})

	user, err := c.Find(ctx, id)
	if err != nil {
		boot.Logger(ctx).Error("user approve failed: " + err.Error())
		return nil, err
	}

	if user.Status != Created {
		boot.Logger(ctx).Error("user approve failed. invalid status: " + user.Status)
		return nil, errors.NewClass("", "").New("").Wrap(goerr.New("invalid user status for approve"))
	}

	user.Status = Processing

	if err := c.repo.Update(ctx, user, AttributeStatus); err != nil {
		return nil, errors.NewClass("", "").New("").Wrap(err)
	}

	if err := boot.Worker.Perform(job.NewUserApproveJob(ctx, user.ID), 0); err != nil {
		return nil, errors.NewClass("", "").New("").Wrap(err)
	}

	return user, nil
}

func (c *Core) Activate(ctx context.Context, id string) errors.IError {
	boot.Logger(ctx).Infow("user activation triggered", map[string]interface{}{"user_id": id})

	user, err := c.Find(ctx, id)
	if err != nil {
		boot.Logger(ctx).Error("user activation failed: " + err.Error())
		return err
	}

	if user.Status != Processing {
		boot.Logger(ctx).Error("user activation failed. invalid status: " + user.Status)
		return errors.NewClass("", "").New("").Wrap(goerr.New("invalid user status for approve"))
	}

	user.Status = Approved

	if err := c.repo.Update(ctx, user, AttributeStatus); err != nil {
		return errors.NewClass("", "").New("").Wrap(err)
	}

	return nil
}

func (c *Core) Delete(ctx context.Context, id string) errors.IError {
	boot.Logger(ctx).Infow("user delete request", map[string]interface{}{"user_id": id})

	user, err := c.Find(ctx, id)
	if err != nil {
		boot.Logger(ctx).Error("user delete failed: " + err.Error())
		return err
	}

	_ = user

	err = c.repo.Delete(ctx, user)
	if err != nil {
		return err
	}

	return nil
}
