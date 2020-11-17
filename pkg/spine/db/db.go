// Package db has specific primitives for database config & connections.
//
// Usage:
// -    E.g. db.NewDb(&c), where c must implement ConfigReader and default use case is to just use Config struct.
package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	otgorm "github.com/smacker/opentracing-gorm"
)

const (
	// PostgresConnectionDSNFormat is postgres connecti	on path format for gorm.
	// E.g. host=localhost:3306 dbname=app sslmode=require user=app password=password
	PostgresConnectionDSNFormat = "host=%s port=%d dbname=%s sslmode=%s user=%s password=%s"

	// MysqlConnectionDSNFormat is mysql connection path format for gorm.
	// E.g. app:password@tcp(localhost:3306)/app?charset=utf8&parseTime=True&loc=Local
	MysqlConnectionDSNFormat = "%s:%s@%s(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local"
)

type contextKey int

const (
	// used to set the db instance in context in case of transactions
	ContextKeyDatabase contextKey = iota
)

var (
	ErrorInvalidArguments = errors.New("invalid arguments given")
)

// ConfigReader interface has methods to read various db configurations.
type ConfigReader interface {
	GetDialect() string
	GetConnectionPath() string
	GetMaxIdleConnections() int
	GetMaxOpenConnections() int
	GetConnMaxLifetime() time.Duration
	GetDebugMode() bool
}

// Config struct holds db configurations and implements ConfigReader.
type Config struct {
	Dialect               string
	Protocol              string
	URL                   string
	Port                  int
	Username              string
	Password              string
	SslMode               string
	Name                  string
	MaxOpenConnections    int
	MaxIdleConnections    int
	ConnectionMaxLifetime time.Duration
	Debug                 bool
}

// GetDialect return dialect.
func (c *Config) GetDialect() string {
	return c.Dialect
}

// GetDebugMode enable or disable debug logs for DB
func (c *Config) GetDebugMode() bool {
	return c.Debug
}

// GetConnectionPath returns connection string to be used by gorm basis dialect.
func (c *Config) GetConnectionPath() string {
	switch c.Dialect {
	case "postgres":
		return fmt.Sprintf(PostgresConnectionDSNFormat, c.URL, c.Port, c.Name, c.SslMode, c.Username, c.Password)
	case "mysql":
		return fmt.Sprintf(MysqlConnectionDSNFormat, c.Username, c.Password, c.Protocol, c.URL, c.Port, c.Name)
	default:
		return ""
	}
}

// GetMaxOpenConnections returns configurable max open connections.
func (c *Config) GetMaxOpenConnections() int {
	return c.MaxOpenConnections
}

// GetMaxIdleConnections returns configurable max idle connections.
func (c *Config) GetMaxIdleConnections() int {
	return c.MaxIdleConnections
}

// GetConnMaxLifetime returns configurable max lifetime.
func (c *Config) GetConnMaxLifetime() time.Duration {
	return c.ConnectionMaxLifetime
}

// Db is the specific wrapper holding gorm db instance.
type DB struct {
	configReader ConfigReader
	instance     *gorm.DB
}

// NewDb instantiates Db and connects to database.
func NewDb(c ConfigReader, args ...interface{}) (*DB, error) {
	db := &DB{configReader: c}
	var driver *sql.DB

	driver = nil

	if len(args) > 0 {
		if len(args) != 1 {
			return nil, ErrorInvalidArguments
		}

		var ok bool

		driver, ok = args[0].(*sql.DB)

		if !ok {
			return nil, ErrorInvalidArguments
		}
	}

	if err := db.connect(driver); err != nil {
		return nil, err
	}

	db.registerCallbacks()

	return db, nil
}

// Instance returns underlying gorm db instance.
// If the transaction in progress then it'll return the db from the context
func (db *DB) Instance(ctx context.Context) *gorm.DB {
	if instance, ok := ctx.Value(ContextKeyDatabase).(*gorm.DB); ok {
		otgorm.SetSpanToGorm(ctx, instance)
		return instance
	}

	otgorm.SetSpanToGorm(ctx, db.instance)
	return db.instance
}

// Alive executes a select query and checks if connection exists and is alive.
func (db *DB) Alive() error {
	return db.instance.DB().Ping()
}

// connect actually opens a gorm connection and configures other connection details.
func (db *DB) connect(driver *sql.DB) error {
	var err error

	if driver == nil {
		if db.instance, err = gorm.Open(db.configReader.GetDialect(), db.configReader.GetConnectionPath()); err != nil {
			return err
		}
	} else {
		if db.instance, err = gorm.Open(db.configReader.GetDialect(), driver); err != nil {
			return err
		}
	}

	db.instance.DB().SetMaxIdleConns(db.configReader.GetMaxIdleConnections())
	db.instance.DB().SetMaxOpenConns(db.configReader.GetMaxOpenConnections())
	db.instance.DB().SetConnMaxLifetime(db.configReader.GetConnMaxLifetime() * time.Second)

	// This will prevent update or delete without where clause
	db.instance.BlockGlobalUpdate(true)

	db.instance.LogMode(db.configReader.GetDebugMode())
	return nil
}

// registerCallbacks replaces registered gorm's callback to touch timestamps
// on create & update in specific way.
func (db *DB) registerCallbacks() {
	replaceGormCallbacks(db)
}
