// +build ignore

package tools

import (
	// build/test tools
	_ "github.com/bykof/go-plantuml"
	_ "github.com/golang/mock/mockgen"
	_ "golang.org/x/lint/golint"

	// protobuf/twirp tools
	_ "github.com/golang/protobuf/protoc-gen-go"
	_ "github.com/twitchtv/twirp/protoc-gen-twirp"
	_ "github.com/uber/prototool/cmd/prototool"
	_ "google.golang.org/grpc"
)
