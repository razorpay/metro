// +build compatibility

package compatibility

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var emulatorClient *pubsub.Client
var metroClient *pubsub.Client

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	setup()
	defer teardown()

	return m.Run()
}

func setup() {
	var err error
	emulatorHost := fmt.Sprintf("%s:8085", os.Getenv("PUBSUB_TEST_HOST"))
	emulatorClient, err = pubsub.NewClient(context.Background(), "project-id",
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)
	if err != nil {
		os.Exit(1)
	}
	metroHost := fmt.Sprintf("%s:8081", os.Getenv("METRO_TEST_HOST"))
	metroClient, err = pubsub.NewClient(context.Background(), "project-id",
		option.WithEndpoint(metroHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)
	if err != nil {
		os.Exit(2)
	}
	// create project in metro
	createProjectInMetro()
}

func createProjectInMetro() {
	url := fmt.Sprintf("http://%s:8082/v1/projects", os.Getenv("METRO_TEST_HOST"))
	r, err := http.Post(url, "application/json", bytes.NewBuffer([]byte("{\"name\": \"project-id\",\"projectId\": \"project-id\"}")))
	if err != nil || r.StatusCode != 200 {
		os.Exit(3)
	}
}

func teardown() {
	// delete project from metro
	url, err := url.Parse(fmt.Sprintf("http://%s:8082/v1/projects/project-id", os.Getenv("METRO_TEST_HOST")))
	if err != nil {
		os.Exit(4)
	}
	req := &http.Request{
		Method: "DELETE",
		URL:    url,
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		os.Exit(5)
	}
}
