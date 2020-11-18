package db_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/razorpay/metro/pkg/spine/db"
)

func TestGetConnectionPath(t *testing.T) {
	c := getDefaultConfig()
	// Asserts connection string for mysql dialect.
	assert.Equal(t, "user:pass@tcp(localhost:3307)/database?charset=utf8&parseTime=True&loc=Local", c.GetConnectionPath())
	// Asserts connection string for postgres dialect.
	c.Dialect = "postgres"
	assert.Equal(t, "host=localhost port=3307 dbname=database sslmode=require user=user password=pass", c.GetConnectionPath())

	// invalid dialect
	c.Dialect = "invalid"
	assert.Equal(t, "", c.GetConnectionPath())
}

func TestNewDb(t *testing.T) {
	tests := []struct {
		name string
		err  string
		args func() ([]interface{}, func())
	}{
		{
			name: "success",
			args: func() ([]interface{}, func()) {
				dbdriver, _, err := sqlmock.New()
				assert.Nil(t, err)

				return []interface{}{dbdriver},
					func() {
						dbdriver.Close()
					}
			},
		},
		{
			name: "more than 1 arg",
			args: func() ([]interface{}, func()) {
				dbdriver, _, err := sqlmock.New()
				assert.Nil(t, err)

				return []interface{}{dbdriver, 123},
					func() {
						dbdriver.Close()
					}
			},
			err: db.ErrorInvalidArguments.Error(),
		},
		{
			name: "more than 1 arg",
			args: func() ([]interface{}, func()) {
				return []interface{}{123, 123},
					func() {}
			},
			err: db.ErrorInvalidArguments.Error(),
		},
		{
			name: "invalid driver",
			args: func() ([]interface{}, func()) {

				return []interface{}{123},
					func() {}
			},
			err: db.ErrorInvalidArguments.Error(),
		},
		{
			name: "nil driver",
			args: func() ([]interface{}, func()) {
				return []interface{}{}, func() {}
			},
			err: `dial tcp .+:3307: connect: connection refused`,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			args, finish := testCase.args()
			defer finish()

			gdb, err := db.NewDb(getDefaultConfig(), args...)

			if testCase.err == "" {
				assert.Nil(t, err)
				assert.NotNil(t, gdb)
			} else {
				expr, e := regexp.Compile(testCase.err)
				assert.Nil(t, e)
				assert.Regexp(t, expr, err.Error())
				assert.Nil(t, gdb)
			}
		})
	}
}

func TestDb_Alive(t *testing.T) {
	mockdriver, mockdb, err := sqlmock.New()
	assert.Nil(t, err)
	defer mockdriver.Close()

	gdb, err := db.NewDb(getDefaultConfig(), mockdriver)
	assert.Nil(t, err)
	assert.NotNil(t, gdb)

	err = gdb.Alive()
	assert.Nil(t, err)

	err = mockdb.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestDB_Instance(t *testing.T) {
	mockdriver, _, err := sqlmock.New()
	assert.Nil(t, err)
	defer mockdriver.Close()

	gdb, err := db.NewDb(getDefaultConfig(), mockdriver)
	assert.Nil(t, err)
	assert.NotNil(t, gdb)

	instance := gdb.Instance(context.TODO())
	assert.NotNil(t, instance)

	ctx := context.WithValue(context.TODO(), db.ContextKeyDatabase, instance)
	tgdb := gdb.Instance(ctx)
	assert.Equal(t, instance, tgdb)
}

func getDefaultConfig() *db.Config {
	return &db.Config{
		Dialect:               "mysql",
		Protocol:              "tcp",
		URL:                   "localhost",
		Port:                  3307,
		Username:              "user",
		Password:              "pass",
		SslMode:               "require",
		Name:                  "database",
		MaxOpenConnections:    5,
		MaxIdleConnections:    5,
		ConnectionMaxLifetime: 0,
	}
}
