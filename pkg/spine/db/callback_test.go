package db_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	"bou.ke/monkey"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/razorpay/metro/pkg/spine/db"
)

type Model struct {
	ID        string
	Name      string
	CreatedAt int64
	UpdatedAt int64
	DeletedAt int64
}

type ModelWithoutTimestamp struct {
	ID   string
	Name string
}

func TestCallback(t *testing.T) {
	staticTime := time.Date(1974, time.May, 19, 1, 2, 3, 4, time.UTC)

	tests := []struct {
		name      string
		err       error
		mock      func(mockDb sqlmock.Sqlmock)
		operation func(db2 *db.DB)
		out       interface{}
	}{
		{
			name: "created and updated time update",
			mock: func(mockDb sqlmock.Sqlmock) {
				mockDb.ExpectBegin()
				mockDb.
					ExpectExec(regexp.QuoteMeta("INSERT INTO `models` "+
						"(`id`,`name`,`created_at`,`updated_at`,`deleted_at`) VALUES "+
						"(?,?,?,?,?)")).
					WithArgs("123", "tester", staticTime.Unix(), staticTime.Unix(), 0).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mockDb.ExpectCommit()
			},
			operation: func(db2 *db.DB) {
				m := Model{
					ID:   "123",
					Name: "tester",
				}

				q := db2.Instance(context.TODO()).Create(&m)
				assert.Nil(t, q.Error)
			},
		},
		{
			name: "created and updated pre-filled time",
			mock: func(mockDb sqlmock.Sqlmock) {
				mockDb.ExpectBegin()
				mockDb.
					ExpectExec(regexp.QuoteMeta("INSERT INTO `models` "+
						"(`id`,`name`,`created_at`,`updated_at`,`deleted_at`) VALUES "+
						"(?,?,?,?,?)")).
					WithArgs("123", "tester", int64(123), staticTime.Unix(), 0).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mockDb.ExpectCommit()
			},
			operation: func(db2 *db.DB) {
				m := Model{
					ID:        "123",
					Name:      "tester",
					CreatedAt: 123,
					UpdatedAt: 123,
				}

				q := db2.Instance(context.TODO()).Create(&m)
				assert.Nil(t, q.Error)
			},
		},
		{
			name: "soft delete",
			mock: func(mockDb sqlmock.Sqlmock) {
				mockDb.ExpectBegin()
				mockDb.
					ExpectExec(regexp.QuoteMeta("UPDATE `models` SET `deleted_at`=138157323 " +
						"WHERE `models`.`deleted_at` IS NULL AND `models`.`id` = ?")).
					WithArgs("123").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mockDb.ExpectCommit()
			},
			operation: func(db2 *db.DB) {
				m := Model{
					ID:        "123",
					Name:      "tester",
					CreatedAt: 123,
					UpdatedAt: 123,
				}

				q := db2.Instance(context.TODO()).Delete(&m)
				assert.Nil(t, q.Error)
			},
		},
		{
			name: "without timestamp fields",
			mock: func(mockDb sqlmock.Sqlmock) {
				mockDb.ExpectBegin()
				mockDb.
					ExpectExec(regexp.QuoteMeta("INSERT INTO `model_without_timestamps` "+
						"(`id`,`name`) VALUES "+
						"(?,?)")).
					WithArgs("123", "tester").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mockDb.ExpectCommit()
			},
			operation: func(db2 *db.DB) {
				m := ModelWithoutTimestamp{
					ID:   "123",
					Name: "tester",
				}

				q := db2.Instance(context.TODO()).Create(&m)
				assert.Nil(t, q.Error)
			},
		},
		{
			name: "hard delete",
			mock: func(mockDb sqlmock.Sqlmock) {
				mockDb.ExpectBegin()
				mockDb.
					ExpectExec(regexp.QuoteMeta("DELETE FROM `model_without_timestamps` " +
						"WHERE `model_without_timestamps`.`id` = ?")).
					WithArgs("123").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mockDb.ExpectCommit()
			},
			operation: func(db2 *db.DB) {
				m := ModelWithoutTimestamp{
					ID:   "123",
					Name: "tester",
				}

				q := db2.Instance(context.TODO()).Delete(&m)
				assert.Nil(t, q.Error)
			},
		},
	}

	pg := monkey.Patch(time.Now, func() time.Time { return staticTime })
	defer pg.Unpatch()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			dbdriver, mockDb, err := sqlmock.New()
			assert.Nil(t, err)
			defer dbdriver.Close()

			testCase.mock(mockDb)

			gdb, err := db.NewDb(getDefaultConfig(), dbdriver)
			assert.Nil(t, err)

			testCase.operation(gdb)
		})
	}
}
