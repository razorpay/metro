// +build ignore

package tools

import (

	// build/test tools
	_ "github.com/bykof/go-plantuml"
	_ "github.com/golang/mock/mockgen"
	_ "golang.org/x/lint/golint"

	// protobuf tools
	_ "github.com/bufbuild/buf/cmd/buf"
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway"
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2"
	_ "github.com/rakyll/statik"
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
