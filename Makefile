# Dir where build binaries are generated. The dir should be gitignored

## Set defaults
export GO111MODULE := on
export PATH := $(GOBIN):$(PATH)

# Fetch OS info
GOVERSION=$(shell go version)
UNAME_OS=$(shell go env GOOS)
UNAME_ARCH=$(shell go env GOARCH)

BUILD_OUT_DIR := "bin/"

METRO_OUT       := "bin/metro"
METRO_MAIN_FILE := "cmd/metro/main.go"

GIT_HOOKS_DIR := "scripts/git_hooks"

# go binary. Change this to experiment with different versions of go.
GO       = go

MODULE   = $(shell $(GO) list -m)
SERVICE  = $(shell basename $(MODULE))

# Proto repo info
PROTO_GIT_URL := "https://github.com/razorpay/metro-proto"
DRONE_PROTO_GIT_URL := "https://$(GIT_TOKEN)@github.com/razorpay/metro-proto"
ifneq ($(GIT_TOKEN),)
PROTO_GIT_URL = $(DRONE_PROTO_GIT_URL)
endif

# Proto gen info
PROTO_ROOT := metro-proto/
RPC_ROOT := rpc/

# Fetch OS info
GOVERSION=$(shell go version)
UNAME_OS=$(shell go env GOOS)
UNAME_ARCH=$(shell go env GOARCH)

VERBOSE = 0
Q 		= $(if $(filter 1,$VERBOSE),,@)
M 		= $(shell printf "\033[34;1m▶\033[0m")


BIN 	 = $(CURDIR)/bin
PKGS     = $(or $(PKG),$(shell $(GO) list ./...))

$(BIN)/%: | $(BIN) ; $(info $(M) building package: $(PACKAGE)…)
	tmp=$$(mktemp -d); \
	   env GOBIN=$(BIN) go get $(PACKAGE) \
		|| ret=$$?; \
	   rm -rf $$tmp ; exit $$ret

$(BIN)/golint: PACKAGE=golang.org/x/lint/golint

GOLINT = $(BIN)/golint

.PHONY: lint
lint: | $(GOLINT) ; $(info $(M) running golint…) @ ## Run golint
	$Q $(GOLINT) -set_exit_status $(PKGS)

.PHONY: fmt
fmt: ; $(info $(M) running gofmt…) @ ## Run gofmt on all source files
	$Q $(GO) fmt $(PKGS)

.PHONY: setup-git-hooks ## First time setup
setup-git-hooks:
	@chmod +x $(GIT_HOOKS_DIR)/*
	@git config core.hooksPath $(GIT_HOOKS_DIR)

.PHONY: all
all: build

.PHONY: deps
deps:
	@echo "\n + Fetching buf dependencies \n"
	# https://github.com/johanbrandhorst/grpc-gateway-boilerplate/blob/master/Makefile
	@go install \
		google.golang.org/protobuf/cmd/protoc-gen-go \
		google.golang.org/grpc/cmd/protoc-gen-go-grpc \
		github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway \
		github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2 \
		github.com/rakyll/statik \
		github.com/bufbuild/buf/cmd/buf
	@go install golang.org/x/lint/golint
	@go install github.com/bykof/go-plantuml
	@go install github.com/golang/mock/mockgen

.PHONY: proto-generate ## Compile protobuf to pb files
proto-generate:
	@echo "\n + Generating pb language bindings\n"
	@buf generate --path ./metro-proto/common
	@buf generate --path ./metro-proto/example
	# Generate static assets for OpenAPI UI
	@statik -m -f -src third_party/OpenAPI/

.PHONY: proto-refresh ## Download and re-compile protobuf
proto-refresh: clean proto-fetch proto-generate ## Fetch proto files frrm remote repo

.PHONY: pre-build
pre-build: clean deps proto-fetch proto-generate mock-gen

.PHONY: build
build: build-info pre-build docker-build

.PHONY: build-info
build-info:
	@echo "\nBuild Info:\n"
	@echo "\t\033[33mOS\033[0m: $(UNAME_OS)"
	@echo "\t\033[33mArch\033[0m: $(UNAME_ARCH)"
	@echo "\t\033[33mGo Version\033[0m: $(GOVERSION)\n"

.PHONY: go-build-api ## Build the binary file for API server
go-build-metro:
	@CGO_ENABLED=1 GOOS=$(UNAME_OS) GOARCH=$(UNAME_ARCH) go build -v -o $(METRO_OUT) $(METRO_MAIN_FILE)

.PHONY: clean ## Remove previous builds, protobuf files, and proto compiled code
clean:
	@echo " + Removing cloned and generated files\n"
	##- todo: use go clean here
	@rm -rf $(METRO_OUT) $(RPC_ROOT)

.PHONY: docker-build
docker-build: docker-build-metro

.PHONY: dev-docker-up ## Bring up docker-compose for local dev-setup
dev-docker-up:
	docker-compose -f deployment/dev/monitoring/docker-compose.yml up -d
	docker-compose -f deployment/dev/docker-compose.yml up -d --build

.PHONY: dev-docker-datastores-up ## Bring up datastore containers
dev-docker-datastores-up:
	docker-compose -f deployment/dev/docker-compose-datastores.yml up -d

.PHONY: dev-docker-datastores-down ## Shut down datastore containers
dev-docker-datastores-down:
	docker-compose -f deployment/dev/docker-compose-datastores.yml down --remove-orphans

.PHONY: dev-docker-down ## Shutdown docker-compose for local dev-setup
dev-docker-down:
	docker-compose -f deployment/dev/docker-compose.yml down --remove-orphans
	docker-compose -f deployment/dev/monitoring/docker-compose.yml down --remove-orphans

.PHONY: docker-build-metro
docker-build-metro:
	@docker build . -f build/docker/Dockerfile --build-arg GIT_TOKEN=${GIT_TOKEN} -t razorpay/metro:latest

.PHONY: mock-gen ## Generates mocks
mock-gen:
	@mkdir -p pkg/queue/mocks
	@mockgen -destination=pkg/worker/mock/manager.go -package=api github.com/razorpay/metro/pkg/worker IManager
	@mockgen -destination=pkg/worker/mock/queue/queue.go -package=queue github.com/razorpay/metro/pkg/worker/queue IQueue
	@mockgen -destination=pkg/worker/mock/logger/logger.go -package=logger github.com/razorpay/metro/pkg/worker ILogger

.PHONY: docs-uml ## Generates UML file
docs-uml:
	@go-plantuml generate --recursive --directories cmd --directories internal --directories pkg --out "docs/uml_graph.puml"

.PHONY: docs ## Generates project documentation
docs: docs-uml

.PHONY: test-unit-drone
test-unit-drone: mock-gen
	@go test -tags=unit -timeout 2m -coverpkg=$(shell comm -23 app.packages unit-test.exclusions | xargs | sed -e 's/ /,/g') -coverprofile=app.cov ./...

.PHONY: test-unit ## Run unit tests
test-unit:
	@touch /tmp/app.packages /tmp/app.cov
	@go list ./... > /tmp/app.packages
	@go test -tags=unit -timeout 2m -coverpkg=$(shell comm -23 /tmp/app.packages unit-test.exclusions | xargs | sed -e 's/ /,/g') -coverprofile=/tmp/app.cov ./...
	@go tool cover -func=/tmp/app.cov

.PHONY: help ## Display this help screen
help:
	@echo "Usage:"
	@grep -E '^\.PHONY: [a-zA-Z_-]+.*?## .*$$' $(MAKEFILE_LIST) | sort | sed 's/\.PHONY\: //' | awk 'BEGIN {FS = " ## "}; {printf "\t\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: help2
help2:
	@grep -hE '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-17s\033[0m %s\n", $$1, $$2}'
