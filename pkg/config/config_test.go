//go:build unit
// +build unit

package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestConfig struct {
	Title string
	Db    TestDbConfig
}

type TestDbConfig struct {
	Dialect               string
	Protocol              string
	Host                  string
	Port                  int
	Username              string
	Password              string
	SslMode               string
	Name                  string
	MaxOpenConnections    int
	MaxIdleConnections    int
	ConnectionMaxLifetime time.Duration
}

func TestLoadConfig(t *testing.T) {
	var c TestConfig

	key := strings.ToUpper("metro") + "_DB_PASSWORD"
	os.Setenv(key, "envpass")
	err := NewConfig(NewOptions("toml", "./testdata", "default")).Load("test", &c)
	assert.Nil(t, err)
	// Asserts that default value exists.
	assert.Equal(t, "mysql", c.Db.Dialect)
	// Asserts that application environment specific value got overridden.
	assert.Equal(t, 10, c.Db.MaxOpenConnections)
	// Asserts that environment variable was honored.
	assert.Equal(t, "envpass", c.Db.Password)
}

func Test_NewDefaultConfig(t *testing.T) {

	workDir := os.Getenv(WorkDirEnv)
	copy := workDir
	defer func() {
		os.Setenv(WorkDirEnv, copy)
	}()

	confirDir := "configdir"
	os.Setenv(WorkDirEnv, confirDir)

	configPath := confirDir + "/config"
	options := NewDefaultConfig()
	assert.NotNil(t, options)
	assert.Equal(t, options.opts.configPath, configPath)
	assert.Equal(t, options.opts.configType, "toml")
	assert.Equal(t, options.opts.defaultConfigFileName, "default")

}
