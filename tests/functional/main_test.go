// +build functional

package functional

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var metroGrpcHost string
var metroHttpHost string
var projectId string
var client *pubsub.Client
var mockServerPushEndpoint string
var mockServerMetricEndpoint string

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

	metroGrpcHost = fmt.Sprintf("%s:8081", os.Getenv("METRO_TEST_HOST"))
	metroHttpHost = fmt.Sprintf("http://%s:8082", os.Getenv("METRO_TEST_HOST"))
	mockServerPushEndpoint = fmt.Sprintf("http://%s:8099/push", os.Getenv("MOCK_SERVER_HOST"))
	mockServerMetricEndpoint = fmt.Sprintf("http://%s:8099/stats", os.Getenv("MOCK_SERVER_HOST"))

	// create project in metro
	setupTestProjects()

	// setup project client
	client, err = pubsub.NewClient(context.Background(), projectId,
		option.WithEndpoint(metroGrpcHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)
	if err != nil {
		os.Exit(5)
	}
}

func setupTestProjects() {
	projectId = fmt.Sprintf("project-%s", uuid.New().String()[0:4])
	url := fmt.Sprintf("%s/v1/projects", metroHttpHost)
	payload := bytes.NewBuffer([]byte("{\"name\": \"" + projectId + "\",\"projectId\": \"" + projectId + "\"}"))
	r, err := http.Post(url, "application/json", payload)
	if err != nil || r.StatusCode != 200 {
		os.Exit(2)
	}
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
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(4)
	}
}
