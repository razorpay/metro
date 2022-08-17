package main

import (
	"context"
	"flag"
	"log"

	"github.com/razorpay/metro/cmd/service/metro"
	"github.com/razorpay/metro/internal/app"
	"github.com/razorpay/metro/internal/boot"
)

var (
	componentName *string
)

func init() {
	componentName = flag.String("component", metro.Web, "component to start")
}

func main() {
	// Initialize context
	ctx := boot.NewContext(context.Background())

	// parse the cmd input
	flag.Parse()

	// read the env
	env := app.GetEnv()

	// Init app dependencies
	metro.Init(ctx, env, *componentName)

	// Run metro
	metro.Run(ctx)
	log.Printf("Metro run initiated...")
}
