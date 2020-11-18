package spine

import (
	"context"
	goErr "errors"

	"github.com/jinzhu/gorm"

	"github.com/razorpay/metro/pkg/errors"
	"github.com/razorpay/metro/pkg/spine/db"
)

type Repo struct {
	Db *db.DB
}

// FindByID fetches the record which matches the ID provided from the entity defined by receiver
// and the result will be loaded into receiver
func (repo Repo) FindByID(ctx context.Context, receiver IModel, id string) errors.IError {
	q := repo.Db.Instance(ctx).Where("id = ?", id).First(receiver)

	return GetDBError(q)
}

// FindByIDs fetches the all the records which matches the IDs provided from the entity defined by receivers
// and the result will be loaded into receivers
func (repo Repo) FindByIDs(ctx context.Context, receivers interface{}, ids []string) errors.IError {
	q := repo.Db.Instance(ctx).Where(AttributeID+" in (?)", ids).Find(receivers)

	return GetDBError(q)
}

// Create inserts a new record in the entity defined by the receiver
// all data filled in the receiver will inserted
func (repo Repo) Create(ctx context.Context, receiver IModel) errors.IError {
	if err := receiver.SetDefaults(); err != nil {
		return err
	}

	if err := receiver.Validate(); err != nil {
		return err
	}

	q := repo.Db.Instance(ctx).Create(receiver)

	return GetDBError(q)
}

// Update will update the given model with respect to primary key / id available in it.
// if the selective list if passed then only defined attributes will be updated along with updated_at time
func (repo Repo) Update(ctx context.Context, receiver IModel, selectiveList ...string) errors.IError {
	q := repo.Db.Instance(ctx).Model(receiver)

	if len(selectiveList) > 0 {
		q = q.Select(selectiveList)
	}

	q = q.Update(receiver)

	if q.RowsAffected == 0 {
		return NoRowAffected.
			New(errNoRowAffected).
			Wrap(goErr.New("no rows have been updated"))
	}

	return GetDBError(q)
}

// Delete deletes the given model
// Soft or hard delete of model depends on the models implementation
// if the model composites SoftDeletableModel then it'll be soft deleted
func (repo Repo) Delete(ctx context.Context, receiver IModel) errors.IError {
	q := repo.Db.Instance(ctx).Delete(receiver)

	return GetDBError(q)
}

// FineMany will fetch multiple records form the entity defined by receiver which matched the condition provided
// note: this wont work for in clause. can be used only for `=` conditions
func (repo Repo) FindMany(
	ctx context.Context,
	receivers interface{},
	condition map[string]interface{}) errors.IError {

	q := repo.Db.Instance(ctx).Where(condition).Find(receivers)

	return GetDBError(q)
}

// Transaction will manage the execution inside a transactions
// adds the txn db in the context for downstream use case
func (repo Repo) Transaction(ctx context.Context, fc func(ctx context.Context) errors.IError) (err errors.IError) {
	// If there is an active transaction then do not create a new transactions
	// use the same and continue
	// *Note: make sure there error occurred in nested transaction should be
	// propagated to outer transaction in this approach
	// as currently we dont have support for savepoint we wont be rollback to particular point
	if _, ok := ctx.Value(db.ContextKeyDatabase).(*gorm.DB); ok {
		return fc(ctx)
	}

	panicked := true
	// start transactions
	tx := repo.Db.Instance(ctx).Begin()

	// post operation check if the operation was successfully completed
	// if failed to complete, then rollback the transaction
	defer func() {
		// Make sure to rollback when panic, Block error or Commit error
		if panicked || err != nil {
			tx.Rollback()
		}
	}()

	// call the transaction handled with tx added in the context key
	err = fc(context.WithValue(ctx, db.ContextKeyDatabase, tx))
	if err == nil {
		err = GetDBError(tx.Commit())
	}

	// if there we no panics then this will be set to false
	// this will be further checked in defer
	panicked = false
	return
}
