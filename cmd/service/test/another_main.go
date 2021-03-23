package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()

	projectID := "project-1234" //fmt.Sprintf("project-%s", uuid.New().String()[0:4])

	//url := "http://localhost:8082/v1/projects"
	//payload := bytes.NewBuffer([]byte("{\"name\": \"" + projectID + "\",\"projectID\": \"" + projectID + "\"}"))
	//r, err := http.Post(url, "application/json", payload)
	//if err != nil || r.StatusCode != 200 {
	//	os.Exit(3)
	//}

	topicName := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])

	metroHost := fmt.Sprintf("%s:8081", "localhost")
	client, err := pubsub.NewClient(context.Background(), projectID,
		option.WithEndpoint(metroHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)

	// ###############################

	topic, err := client.CreateTopic(context.Background(), topicName)
	if err != nil {
		fmt.Println("error in create subscription", err)
		os.Exit(1)
	}

	i := 0

	// run infinite producer
	go func() {
		for {

			payload := fmt.Sprintf("payload %d", i)
			r := topic.Publish(context.Background(), &pubsub.Message{Data: []byte(payload)})
			r.Get(context.Background())
			i++

			fmt.Println("producing message : ", payload)
			time.Sleep(5 * time.Second)
		}
	}()

	sub, err := client.CreateSubscription(context.Background(), subscription, pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		fmt.Println("error in create subscription:", err)
		os.Exit(2)
	}

	// ###############################
	sub.ReceiveSettings.NumGoroutines = 2
	fmt.Println("starting go-1...")
	go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		fmt.Println("go-1 : reading messages", m.ID, string(m.Data))

		if time.Now().Unix()%2 == 0 {
			fmt.Println("go-1 : Ack-ing message", m.ID, string(m.Data))
			m.Ack()
		} else {
			fmt.Println("go-1 : Nack-ing message", m.ID, string(m.Data))
			m.Nack()
		}

		time.Sleep(time.Millisecond * 500)
	})

	// start the second consumer after some time
	//time.Sleep(time.Second * 10)

	//fmt.Println("starting go-2...")
	//go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
	//	fmt.Println("go-2 : reading messages", m.ID, string(m.Data))
	//	m.Ack()
	//
	//	time.Sleep(time.Second * 2)
	//})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM)

	<-quit
}
