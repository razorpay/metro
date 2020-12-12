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
	componentName = flag.String("component", metro.Producer, "component to start")
}

func main() {
	// Initialize context
	ctx, cancel := context.WithCancel(boot.NewContext(context.Background()))
	defer cancel()

	// parse the cmd input
	flag.Parse()

	// read the env
	env := boot.GetEnv()

	// Init app dependencies
	metro.Init(env, *componentName)

	// Run metro
	metro.Run(ctx)
}
