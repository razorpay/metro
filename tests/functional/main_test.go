// +build functional

package functional

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"

	configreader "github.com/razorpay/metro/pkg/config"
	metro_pubsub "github.com/razorpay/metro/tests/metroclient"
	"github.com/razorpay/metro/tests/pushserver"
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
var orderedTopic string
var orderedSub string
var ps *pushserver.PushServer
var chanMap = map[string]chan pushserver.PushMessage{}

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
		os.Exit(1)
	}
	if _, ok := appConfig["admin"]; ok {
		adminUser = appConfig["admin"].(map[string]interface{})["username"].(string)
		adminPassword = appConfig["admin"].(map[string]interface{})["password"].(string)
	}

	metroGrpcHost = fmt.Sprintf("%s:8081", os.Getenv("METRO_TEST_HOST"))
	metroHttpHost = fmt.Sprintf("http://%s:8082", os.Getenv("METRO_TEST_HOST"))
	mockServerPushEndpoint = fmt.Sprintf("http://%s:8077/push", os.Getenv("MOCK_SERVER_HOST"))
	mockServerMetricEndpoint = fmt.Sprintf("http://%s:8099/stats", os.Getenv("MOCK_SERVER_HOST"))

	// create project in metro
	setupTestProjects()

	client, err = metro_pubsub.NewMetroClient(metroGrpcHost, false, projectId, metro_pubsub.Credentials{Username: username, Password: password})
	if err != nil {
		os.Exit(2)
	}
	setupOrdering()
	ps = pushserver.StartServer(context.TODO(), chanMap)
}

func setupTestProjects() {
	projectId = fmt.Sprintf("project-%s", uuid.New().String()[0:4])
	url := fmt.Sprintf("%s/v1/projects", metroHttpHost)
	payload := bytes.NewBuffer([]byte("{\"name\": \"" + projectId + "\",\"projectId\": \"" + projectId + "\"}"))
	req, err := http.NewRequest(http.MethodPost, url, payload)
	if err != nil {
		os.Exit(3)
	}
	req.SetBasicAuth(adminUser, adminPassword)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(4)
	}
	setupProjectCredentials()
}

func setupProjectCredentials() {
	var parsedResponse map[string]string

	url := fmt.Sprintf("%s/v1/projects/%s/credentials", metroHttpHost, projectId)
	payload := bytes.NewBuffer([]byte(fmt.Sprintf(`{"username": "%s", "password":"password"}`, projectId+"_user")))
	req, err := http.NewRequest(http.MethodPost, url, payload)
	if err != nil {
		os.Exit(5)
	}
	req.SetBasicAuth(adminUser, adminPassword)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(6)
	}
	defer r.Body.Close()
	if err = json.NewDecoder(r.Body).Decode(&parsedResponse); err != nil {
		os.Exit(7)
	}
	password = parsedResponse["password"]
	username = parsedResponse["username"]
}

func teardown() {
	// delete project from metro
	url := fmt.Sprintf("%s/v1/projects/%s", metroHttpHost, projectId)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		os.Exit(8)
	}
	req.SetBasicAuth(adminUser, adminPassword)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(9)
	}
	teardownOrdering()
	ps.StopServer()
}

func setupOrdering() {
	orderedTopic = fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	orderedSub = fmt.Sprintf("subscription-%s", uuid.New().String()[0:4])

	topic, err := client.CreateTopic(context.Background(), orderedTopic)
	if err != nil {
		os.Exit(10)
	}

	if _, err := client.CreateSubscription(context.Background(), orderedSub,
		pubsub.SubscriptionConfig{
			Topic: topic,
			PushConfig: pubsub.PushConfig{
				Endpoint: mockServerPushEndpoint,
			},
			DeadLetterPolicy: &pubsub.DeadLetterPolicy{
				MaxDeliveryAttempts: 2,
			},
			RetryPolicy: &pubsub.RetryPolicy{
				MinimumBackoff: 1 * time.Second,
				MaximumBackoff: 1 * time.Second,
			},
			EnableMessageOrdering: true,
		}); err != nil {
		os.Exit(11)
	}

	chanMap[fmt.Sprintf("projects/%s/subscriptions/%s", projectId, orderedSub)] = make(chan pushserver.PushMessage)
	time.Sleep(time.Duration(time.Second * 5))
}

func teardownOrdering() {
	client.Topic(orderedTopic).Delete(context.TODO())
	client.Subscription(orderedSub).Delete(context.TODO())
}
