package main

import (
	"context"
	"flag"

	"github.com/razorpay/metro/cmd/service/metro"
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
	env := boot.GetEnv()

	// Init app dependencies
	metro.Init(ctx, env, *componentName)

	// Run metro
	metro.Run(ctx)
}
