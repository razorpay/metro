# Dir where build binaries are generated. The dir should be gitignored
BUILD_OUT_DIR := "bin/"

API_OUT       := "bin/api"
API_MAIN_FILE := "cmd/api/main.go"

WORKER_OUT       := "bin/worker"
WORKER_MAIN_FILE := "cmd/worker/main.go"

MIGRATION_OUT       := "bin/migration"
MIGRATION_MAIN_FILE := "cmd/migration/main.go"

GIT_HOOKS_DIR := "scripts/git_hooks"

# go binary. Change this to experiment with different versions of go.
GO       = go

MODULE   = $(shell $(GO) list -m)
SERVICE  = $(shell basename $(MODULE))

# Proto repo info
PROTO_GIT_URL := "https://github.com/razorpay/proto"
DRONE_PROTO_GIT_URL := "https://$(GIT_TOKEN)@github.com/razorpay/proto"
ifneq ($(GIT_TOKEN),)
PROTO_GIT_URL = $(DRONE_PROTO_GIT_URL)
endif

# Accept a branch from which we need to checkout proto files. Useful for dev testing.
PROTO_BRANCH ?= master

# Proto gen info
PROTO_ROOT := proto/
RPC_ROOT := rpc/

# Fetch OS info
GOVERSION=$(shell go version)
UNAME_OS=$(shell go env GOOS)
UNAME_ARCH=$(shell go env GOARCH)

# This is the only variable that ever should change.
# This can be a branch, tag, or commit.
# When changed, the given version of Prototool will be installed to
# .tmp/$(uname -s)/(uname -m)/bin/prototool
PROTOTOOL_VERSION := v1.9.0
PROTOC_GEN_GO_VERSION := v1.3.2
PROTOC_GEN_TWIRP_VERSION := v5.10.1

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

.PHONY: proto-fetch ## Fetch proto files frrm remote repo
proto-fetch: ## Fetch proto files frrm remote repo
	@echo "\n + Fetching proto files from branch: $(PROTO_BRANCH) \n"
	@mkdir $(PROTO_ROOT) && \
	cd $(PROTO_ROOT) && \
	git init --quiet && \
	git config core.sparseCheckout true && \
	cp $(CURDIR)/scripts/proto_modules .git/info/sparse-checkout && \
	git remote add origin $(PROTO_GIT_URL)  && \
	git fetch origin $(PROTO_BRANCH) --quiet && \
	git checkout origin/$(PROTO_BRANCH) --quiet

.PHONY: all
all: build

.PHONY: deps
deps:
	@echo "\n + Fetching dependencies \n"
	@go get google.golang.org/grpc
	@go get github.com/uber/prototool/cmd/prototool@$(PROTOTOOL_VERSION)
	@go get github.com/golang/protobuf/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	@go get github.com/twitchtv/twirp/protoc-gen-twirp@$(PROTOC_GEN_TWIRP_VERSION)
	@go get golang.org/x/lint/golint
	@go get github.com/bykof/go-plantuml
	@go get github.com/golang/mock/mockgen

.PHONY: proto-generate ## Compile protobuf to pb and twirp files
proto-generate:
	@echo "\n + Generating pb language bindings\n"
	@prototool files $(PROTO_ROOT)
	@prototool generate $(PROTO_ROOT)

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
go-build-api:
	@CGO_ENABLED=0 GOOS=$(UNAME_OS) GOARCH=$(UNAME_ARCH) go build -i -v -o $(API_OUT) $(API_MAIN_FILE)

.PHONY: go-build-worker ## Build the binary file for the worker
go-build-worker:
	@CGO_ENABLED=0 GOOS=$(UNAME_OS) GOARCH=$(UNAME_ARCH) go build -i -v -o $(WORKER_OUT) $(WORKER_MAIN_FILE)

.PHONY: go-build-migration ## Build the binary file for database migrations
go-build-migration:
	@CGO_ENABLED=0 GOOS=$(UNAME_OS) GOARCH=$(UNAME_ARCH) go build -i -v -o $(MIGRATION_OUT) $(MIGRATION_MAIN_FILE)

.PHONY: clean ## Remove previous builds, protobuf files, and proto compiled code
clean:
	@echo " + Removing cloned and generated files\n"
	##- todo: use go clean here
	@rm -rf $(API_OUT) $(MIGRATION_OUT) $(WORKER_OUT) $(PROTO_ROOT) $(RPC_ROOT)

.PHONY: docker-build
docker-build: docker-build-api docker-build-worker docker-build-migration

.PHONY: dev-docker-up ## Bring up docker-compose for local dev-setup
dev-docker-up:
	docker-compose -f deployment/dev/monitoring/docker-compose.yml up -d
	docker-compose -f deployment/dev/docker-compose.yml up -d --build

.PHONY: dev-docker-rebuild ## Rebuild
dev-docker-rebuild:
	docker-compose -f deployment/dev/monitoring/docker-compose.yml up -d
	docker-compose -f deployment/dev/docker-compose.yml up -d --build

.PHONY: dev-docker-down ## Shutdown docker-compose for local dev-setup
dev-docker-down:
	docker-compose -f deployment/dev/docker-compose.yml down --remove-orphans
	docker-compose -f deployment/dev/monitoring/docker-compose.yml down --remove-orphans

.PHONY: docker-build-api
docker-build-api:
	@docker build . -f build/docker/prod/Dockerfile.api --build-arg GIT_TOKEN=${GIT_TOKEN} -t razorpay/app_name:latest

.PHONY: docker-build-worker
docker-build-worker:
	@docker build . -f build/docker/prod/Dockerfile.webhook.worker --build-arg GIT_TOKEN=${GIT_TOKEN} -t razorpay/app_name-worker:latest

.PHONY: docker-build-migration
docker-build-migration:
	@docker build . -f build/docker/prod/Dockerfile.migration --build-arg GIT_TOKEN=${GIT_TOKEN} -t razorpay/app_name-migration:latest

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
