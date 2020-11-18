package spine_test

import (
	"context"
	goErr "errors"
	"regexp"
	"testing"
	"time"

	"bou.ke/monkey"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/razorpay/metro/pkg/errors"
	"github.com/razorpay/metro/pkg/spine"
	"github.com/razorpay/metro/pkg/spine/db"
)

type TestModel struct {
	spine.Model
	Name string `json:"name"`
}

func (t *TestModel) EntityName() string {
	return "model"
}

func (t *TestModel) TableName() string {
	return "model"
}

func (t *TestModel) GetID() string {
	return t.ID
}

func (t *TestModel) Validate() errors.IError {
	return nil
}

func (t *TestModel) SetDefaults() errors.IError {
	return nil
}

type InvalidModel struct {
	TestModel
}

func (t *InvalidModel) Validate() errors.IError {
	return spine.GetValidationError(goErr.New("error"))
}

type SoftDeleteModel struct {
	TestModel
	DeletedAt int64
}

func TestFindByID(t *testing.T) {
	repo, mockdb := createRepo(t)

	mockdb.
		ExpectQuery(regexp.QuoteMeta("SELECT * FROM `model` WHERE (id = ?) ORDER BY `model`.`id` ASC LIMIT 1")).
		WithArgs("1").
		WillReturnRows(
			sqlmock.
				NewRows([]string{"id", "name"}).
				AddRow(1, "test"))

	model := TestModel{}
	err := repo.FindByID(context.TODO(), &model, "1")
	assert.Nil(t, err)
	assert.Equal(t, model.Name, "test")
}

func TestFindByIDs(t *testing.T) {
	repo, mockdb := createRepo(t)

	mockdb.
		ExpectQuery(regexp.QuoteMeta("SELECT * FROM `model`")).
		WithArgs("1").
		WillReturnRows(
			sqlmock.
				NewRows([]string{"id", "name"}).
				AddRow("1", "test"))

	var models []TestModel
	err := repo.FindByIDs(context.TODO(), &models, []string{"1"})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(models))
}

func TestCreate(t *testing.T) {
	repo, mockdb := createRepo(t)

	staticTime := time.Date(2020, time.May, 19, 1, 2, 3, 4, time.UTC)

	mockdb.ExpectBegin()
	mockdb.
		ExpectExec(
			regexp.QuoteMeta("INSERT INTO `model` (`id`,`created_at`,`updated_at`,`name`) VALUES (?,?,?,?)")).
		WithArgs(sqlmock.AnyArg(), staticTime.Unix(), staticTime.Unix(), "test").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockdb.ExpectCommit()

	model := TestModel{
		Name: "test",
	}

	pg := monkey.Patch(time.Now, func() time.Time { return staticTime })
	defer pg.Unpatch()

	err := repo.Create(context.TODO(), &model)
	assert.Nil(t, err)
	assert.Equal(t, "test", model.Name)
	assert.Equal(t, staticTime.Unix(), model.GetCreatedAt())
	assert.Equal(t, staticTime.Unix(), model.GetUpdatedAt())
}

func TestCreate_ValidationFailure(t *testing.T) {
	repo, _ := createRepo(t)

	model := InvalidModel{}

	err := repo.Create(context.TODO(), &model)
	assert.NotNil(t, err)
	assert.Equal(t, "validation_failure: error", err.Error())
}

func TestTransactionCommit(t *testing.T) {
	repo, mockdb := createRepo(t)
	mockdb.ExpectBegin()
	mockdb.ExpectCommit()

	err := repo.Transaction(context.TODO(), func(ctx context.Context) errors.IError {
		return nil
	})

	assert.Nil(t, err)
}

func TestTransactionRollback(t *testing.T) {
	repo, mockdb := createRepo(t)
	mockdb.ExpectBegin()
	mockdb.ExpectRollback()

	err := repo.Transaction(context.TODO(), func(ctx context.Context) errors.IError {
		return spine.DBError.New("failed to execute query")
	})

	assert.Equal(t, "db_error: failed to execute query", err.Error())
}

func TestUpdate(t *testing.T) {
	repo, mockdb := createRepo(t)

	staticTime := time.Date(2020, time.May, 19, 1, 2, 3, 4, time.UTC)

	mockdb.ExpectBegin()
	mockdb.
		ExpectExec(regexp.QuoteMeta("UPDATE `model` SET `created_at` = ?, `id` = ?, `name` = ?, `updated_at` = ? WHERE `model`.`id` = ?")).
		WithArgs(int64(123), "1", "test", 1589850123, "1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockdb.ExpectCommit()

	model := TestModel{
		Model: spine.Model{
			ID:        "1",
			CreatedAt: int64(123),
		},
		Name: "test",
	}

	pg := monkey.Patch(time.Now, func() time.Time { return staticTime })
	defer pg.Unpatch()

	err := repo.Update(context.TODO(), &model)
	assert.Nil(t, err)
	assert.Equal(t, "test", model.Name)
	assert.Equal(t, int64(123), model.CreatedAt)
	assert.Equal(t, staticTime.Unix(), model.UpdatedAt)
}

func TestDelete_HardDelete(t *testing.T) {
	repo, mockdb := createRepo(t)

	mockdb.ExpectBegin()
	mockdb.
		ExpectExec(regexp.QuoteMeta("DELETE FROM `model` WHERE `model`.`id` = ?")).
		WithArgs("1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockdb.ExpectCommit()

	model := TestModel{
		Model: spine.Model{
			ID:        "1",
			CreatedAt: int64(123),
		},
		Name: "test",
	}

	err := repo.Delete(context.TODO(), &model)
	assert.Nil(t, err)
}

func TestDelete_SoftDelete(t *testing.T) {
	repo, mockdb := createRepo(t)

	staticTime := time.Date(2020, time.May, 19, 1, 2, 3, 4, time.UTC)

	mockdb.ExpectBegin()
	mockdb.
		ExpectExec(regexp.QuoteMeta("UPDATE `model` SET `deleted_at`=1589850123 WHERE `model`.`deleted_at` IS NULL AND `model`.`id` = ?")).
		WithArgs("1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockdb.ExpectCommit()

	model := SoftDeleteModel{
		TestModel: TestModel{
			Model: spine.Model{
				ID: "1",
			},
		},
	}

	pg := monkey.Patch(time.Now, func() time.Time { return staticTime })
	defer pg.Unpatch()

	err := repo.Delete(context.TODO(), &model)
	assert.Nil(t, err)
}

func TestUpdateNoRowAffected(t *testing.T) {
	repo, mockdb := createRepo(t)

	staticTime := time.Date(2020, time.May, 19, 1, 2, 3, 4, time.UTC)

	mockdb.ExpectBegin()
	mockdb.
		ExpectExec(regexp.QuoteMeta("UPDATE `model` SET `created_at` = ?, `id` = ?, `name` = ?, `updated_at` = ? WHERE `model`.`id` = ?")).
		WithArgs(int64(123), "1", "test", 1589850123, "1")
	mockdb.ExpectCommit()

	model := TestModel{
		Model: spine.Model{
			ID:        "1",
			CreatedAt: int64(123),
		},
		Name: "test",
	}

	pg := monkey.Patch(time.Now, func() time.Time { return staticTime })
	defer pg.Unpatch()

	err := repo.Update(context.TODO(), &model)
	assert.NotNil(t, err)
	assert.Equal(t, spine.NoRowAffected, err.Class())
}

func TestUpdateSelective(t *testing.T) {
	repo, mockdb := createRepo(t)

	staticTime := time.Date(2020, time.May, 19, 1, 2, 3, 4, time.UTC)

	mockdb.ExpectBegin()
	mockdb.
		ExpectExec(regexp.QuoteMeta("UPDATE `model` SET `name` = ?, `updated_at` = ? WHERE `model`.`id` = ?")).
		WithArgs("test", 1589850123, "1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockdb.ExpectCommit()

	model := TestModel{
		Model: spine.Model{
			ID:        "1",
			CreatedAt: int64(123),
		},
		Name: "test",
	}

	pg := monkey.Patch(time.Now, func() time.Time { return staticTime })
	defer pg.Unpatch()

	err := repo.Update(context.TODO(), &model, "name")
	assert.Nil(t, err)
	assert.Equal(t, "test", model.Name)
	assert.Equal(t, int64(123), model.CreatedAt)
	assert.Equal(t, staticTime.Unix(), model.UpdatedAt)
}

func TestFindMany(t *testing.T) {
	repo, mockdb := createRepo(t)

	mockdb.
		ExpectQuery(regexp.QuoteMeta("SELECT * FROM `model`")).
		WillReturnRows(
			sqlmock.
				NewRows([]string{"id", "name"}).
				AddRow("1", "test").
				AddRow("2", "test2"))

	var models []TestModel

	err := repo.FindMany(context.TODO(), &models, map[string]interface{}{})
	assert.Nil(t, err)
	model := models[0]
	assert.Equal(t, "1", model.ID)
	model = models[1]
	assert.Equal(t, "2", model.ID)
}

func TestTransaction_Nested(t *testing.T) {
	repo, mockdb := createRepo(t)
	mockdb.ExpectBegin()
	mockdb.ExpectCommit()

	err := repo.Transaction(context.TODO(), func(ctx context.Context) errors.IError {
		res := repo.Transaction(ctx, func(ctx context.Context) errors.IError {
			return nil
		})
		return res
	})

	assert.Nil(t, err)
}

func createRepo(t *testing.T) (spine.Repo, sqlmock.Sqlmock) {
	mockdriver, mockdb, err := sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	dbInstance, err := db.NewDb(getDefaultConfig(), mockdriver)
	assert.Nil(t, err)

	return spine.Repo{Db: dbInstance}, mockdb
}

func getDefaultConfig() *db.Config {
	return &db.Config{
		Dialect:               "mysql",
		Protocol:              "tcp",
		URL:                   "localhost:3306",
		Username:              "user",
		Password:              "pass",
		SslMode:               "require",
		Name:                  "database",
		MaxOpenConnections:    5,
		MaxIdleConnections:    5,
		ConnectionMaxLifetime: 0,
		Debug:                 true,
	}
}
