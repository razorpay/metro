// +build compatibility

package compatibility

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"cloud.google.com/go/pubsub"
	configreader "github.com/razorpay/metro/pkg/config"
	metro_pubsub "github.com/razorpay/metro/tests/metroclient"
)

var emulatorHost string
var metroHost string
var metroHTTPHost string
var emulatorClient *pubsub.Client
var metroClient *pubsub.Client
var projectId string
var adminUser string
var adminPassword string
var username string
var password string

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	projectId = fmt.Sprintf("project-%s", uuid.New().String()[0:4])

	setup()
	defer teardown()

	return m.Run()
}

func setup() {
	var err error

	env := os.Getenv("APP_ENV")
	if env == "" {
		env = "dev"
	}

	var appConfig map[string]interface{}
	if err = configreader.NewDefaultConfig().Load(env, &appConfig); err != nil {
		os.Exit(1)
	}
	if _, ok := appConfig["admin"]; ok {
		adminUser = appConfig["admin"].(map[string]interface{})["username"].(string)
		adminPassword = appConfig["admin"].(map[string]interface{})["password"].(string)
	}

	emulatorHost = fmt.Sprintf("%s:8085", os.Getenv("PUBSUB_TEST_HOST"))
	metroHost = fmt.Sprintf("%s:8081", os.Getenv("METRO_TEST_HOST"))
	metroHTTPHost = fmt.Sprintf("http://%s:8082", os.Getenv("METRO_TEST_HOST"))

	createProjectInMetro()

	emulatorClient, err = pubsub.NewClient(context.Background(), projectId,
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)
	if err != nil {
		os.Exit(2)
	}

	metroClient, err = metro_pubsub.NewMetroClient(metroHost, false, projectId, metro_pubsub.Credentials{Username: username, Password: password})
	if err != nil {
		os.Exit(3)
	}
}

func createProjectInMetro() {
	url := fmt.Sprintf("%s/v1/projects", metroHTTPHost)
	payload := bytes.NewBuffer([]byte("{\"name\": \"" + projectId + "\",\"projectId\": \"" + projectId + "\"}"))
	req, err := http.NewRequest(http.MethodPost, url, payload)
	if err != nil {
		os.Exit(4)
	}
	req.SetBasicAuth(adminUser, adminPassword)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(5)
	}
	setupProjectCredentials()
}

func setupProjectCredentials() {
	var parsedResponse map[string]string

	url := fmt.Sprintf("%s/v1/projects/%s/credentials", metroHTTPHost, projectId)
	payload := bytes.NewBuffer([]byte(fmt.Sprintf(`{"username": "%s", "password":"password"}`, projectId+"_user")))
	req, err := http.NewRequest(http.MethodPost, url, payload)
	if err != nil {
		os.Exit(6)
	}
	req.SetBasicAuth(adminUser, adminPassword)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(7)
	}
	defer r.Body.Close()
	if err = json.NewDecoder(r.Body).Decode(&parsedResponse); err != nil {
		os.Exit(8)
	}
	password = parsedResponse["password"]
	username = parsedResponse["username"]
}

func teardown() {
	// delete project from metro
	url := fmt.Sprintf("%s/v1/projects/%s", metroHTTPHost, projectId)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		os.Exit(9)
	}
	req.SetBasicAuth(adminUser, adminPassword)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(10)
	}
}
