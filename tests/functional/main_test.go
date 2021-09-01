// +build functional

package functional

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	configreader "github.com/razorpay/metro/pkg/config"
	metro_pubsub "github.com/razorpay/metro/tests/metro-pubsub"
)

var metroGrpcHost string
var metroHttpHost string
var projectId string
var client *pubsub.Client
var mockServerPushEndpoint string
var mockServerMetricEndpoint string
var adminUser string
var adminPassword string
var username string
var password string

func TestMain(m *testing.M) {
	// all pretest setup
	setup()

	// post test cleanup
	defer teardown()

	// runs all the tests under the functional package
	exitVal := m.Run()
	os.Exit(exitVal)
}

func setup() {
	var err error

	env := os.Getenv("APP_ENV")
	if env == "" {
		env = "dev"
	}

	var appConfig map[string]interface{}
	if err = configreader.NewDefaultConfig().Load(env, &appConfig); err != nil {
		os.Exit(5)
	}
	if _, ok := appConfig["admin"]; ok {
		adminUser = appConfig["admin"].(map[string]interface{})["username"].(string)
		adminPassword = appConfig["admin"].(map[string]interface{})["password"].(string)
	}

	metroGrpcHost = fmt.Sprintf("%s:8081", os.Getenv("METRO_TEST_HOST"))
	metroHttpHost = fmt.Sprintf("http://%s:8082", os.Getenv("METRO_TEST_HOST"))
	mockServerPushEndpoint = fmt.Sprintf("http://%s:8099/push", os.Getenv("MOCK_SERVER_HOST"))
	mockServerMetricEndpoint = fmt.Sprintf("http://%s:8099/stats", os.Getenv("MOCK_SERVER_HOST"))

	// create project in metro
	setupTestProjects()

	client, err = metro_pubsub.NewMetroClient(metroGrpcHost, false, projectId, metro_pubsub.Credentials{Username: username, Password: password})
	if err != nil {
		os.Exit(5)
	}
}

func setupTestProjects() {
	projectId = fmt.Sprintf("project-%s", uuid.New().String()[0:4])
	url := fmt.Sprintf("%s/v1/projects", metroHttpHost)
	payload := bytes.NewBuffer([]byte("{\"name\": \"" + projectId + "\",\"projectId\": \"" + projectId + "\"}"))
	req, err := http.NewRequest(http.MethodPost, url, payload)
	if err != nil {
		os.Exit(2)
	}
	req.SetBasicAuth(adminUser, adminPassword)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(2)
	}
	setupProjectCredentials()
}

func setupProjectCredentials() {
	var parsedResponse map[string]string

	url := fmt.Sprintf("%s/v1/projects/%s/credentials", metroHttpHost, projectId)
	payload := bytes.NewBuffer([]byte(fmt.Sprintf(`{"username": "%s", "password":"password"}`, projectId+"_user")))
	req, err := http.NewRequest(http.MethodPost, url, payload)
	if err != nil {
		os.Exit(2)
	}
	req.SetBasicAuth(adminUser, adminPassword)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(2)
	}
	defer r.Body.Close()
	if err = json.NewDecoder(r.Body).Decode(&parsedResponse); err != nil {
		os.Exit(2)
	}
	password = parsedResponse["password"]
	username = parsedResponse["username"]
	fmt.Println(password)
}

func teardown() {
	// delete project from metro
	url, err := url.Parse(fmt.Sprintf("%s/v1/projects/%s", metroHttpHost, projectId))
	if err != nil {
		os.Exit(3)
	}
	req := &http.Request{
		Method: "DELETE",
		URL:    url,
	}
	req.SetBasicAuth(adminUser, adminPassword)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(4)
	}
}
