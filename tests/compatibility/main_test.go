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

	"github.com/google/uuid"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var emulatorClient *pubsub.Client
var metroClient *pubsub.Client
var projectId string

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
	emulatorHost := fmt.Sprintf("%s:8085", os.Getenv("PUBSUB_TEST_HOST"))
	emulatorClient, err = pubsub.NewClient(context.Background(), projectId,
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)
	if err != nil {
		fmt.Println("error 1 ", err)
		os.Exit(1)
	}
	metroHost := fmt.Sprintf("%s:8081", os.Getenv("METRO_TEST_HOST"))
	metroClient, err = pubsub.NewClient(context.Background(), projectId,
		option.WithEndpoint(metroHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)
	if err != nil {
		fmt.Println("error 2 ", err)
		os.Exit(2)
	}
	// create project in metro
	createProjectInMetro()
}

func createProjectInMetro() {
	url := fmt.Sprintf("http://%s:8082/v1/projects", os.Getenv("METRO_TEST_HOST"))
	payload := bytes.NewBuffer([]byte("{\"name\": \"" + projectId + "\",\"projectId\": \"" + projectId + "\"}"))
	fmt.Println("payload", payload)
	r, err := http.Post(url, "application/json", payload)
	if err != nil || r.StatusCode != 200 {
		fmt.Println("error 3", err)
		os.Exit(3)
	}
}

func teardown() {
	// delete project from metro
	url, err := url.Parse(fmt.Sprintf("http://%s:8082/v1/projects/%s", os.Getenv("METRO_TEST_HOST"), projectId))
	if err != nil {
		fmt.Println("error 4", err)
		os.Exit(4)
	}
	req := &http.Request{
		Method: "DELETE",
		URL:    url,
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		fmt.Println("error 5", err)
		os.Exit(5)
	}
}
