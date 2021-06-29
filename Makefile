## Set defaults
export GO111MODULE := on

# Fetch OS info
GOVERSION=$(shell go version)
UNAME_OS=$(shell go env GOOS)
UNAME_ARCH=$(shell go env GOARCH)

METRO_OUT       := "bin/metro"
METRO_MAIN_FILE := "cmd/service/main.go"

GIT_HOOKS_DIR := "scripts/git_hooks"
TMP_DIR := ".tmp"
DOCS_DIR := "docs"
UML_OUT_FILE := "uml_graph.puml"
PKG_LIST_TMP_FILE := "app.packages"
UNIT_COVERAGE_TMP_FILE := "app-unit.cov"
INTG_COVERAGE_TMP_FILE := "app-integration.cov"
UNIT_TEST_EXCLUSIONS_FILE := "unit-test.exclusions"
# Proto gen info
PROTO_ROOT := "metro-proto/"
RPC_ROOT := "rpc/"


BUF_BIN := /usr/local/bin
BUF_VERSION := 0.43.2
BUF_BINARY_NAME := buf
BUF_UNAME_OS := $(shell uname -s)
BUF_UNAME_ARCH := $(shell uname -m)

# go binary. Change this to experiment with different versions of go.
GO       = go

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

$(BIN)/goimports: PACKAGE=golang.org/x/tools/cmd/goimports

GOIMPORTS = $(BIN)/goimports

GOFILES     ?= $(shell find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./statik/*" -not -path "./rpc/*" -not -path "*/mocks/*")

.PHONY: goimports ## Run goimports and format files
goimports: | $(GOIMPORTS) ; $(info $(M) running goimports…) @
	$Q $(GOIMPORTS) -w $(GOFILES)

.PHONY: goimports-check ## Check goimports without modifying the files
goimports-check: | $(GOIMPORTS) ; $(info $(M) running goimports -l …) @
	$(eval FILES=$(shell sh -c '$(GOIMPORTS) -l $(GOFILES)'))
	@$(if $(strip $(FILES)), echo $(FILES); exit 1, echo "goimports check passed")

$(BIN)/golint: PACKAGE=golang.org/x/lint/golint

GOLINT = $(BIN)/golint

.PHONY: lint-check ## Run golint check
lint-check: | $(GOLINT) ; $(info $(M) running golint…) @
	$Q $(GOLINT) -set_exit_status $(PKGS)

.PHONY: setup-git-hooks ## First time setup
setup-git-hooks:
	@chmod +x $(GIT_HOOKS_DIR)/*
	@git config core.hooksPath $(GIT_HOOKS_DIR)

.PHONY: deps ## Fetch dependencies
deps: buf-deps
	@echo "\n + Fetching buf dependencies \n"
	# https://github.com/johanbrandhorst/grpc-gateway-boilerplate/blob/master/Makefile
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.26.0
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.5.0
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.5.0
	@go install github.com/rakyll/statik@v0.1.7
	@go install golang.org/x/lint/golint@latest
	@go install github.com/bykof/go-plantuml@v1.0.0
	@go install github.com/golang/mock/mockgen@v1.6.0

buf-deps:
	curl -sSL \
	https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/${BUF_BINARY_NAME}-$(BUF_UNAME_OS)-$(BUF_UNAME_ARCH) \
	-o ${BUF_BIN}/${BUF_BINARY_NAME} && \
	chmod +x ${BUF_BIN}/${BUF_BINARY_NAME}

.PHONY: proto-generate ## Compile protobuf to pb files
proto-generate:
	@echo "\n + Generating pb language bindings\n"
	@buf generate --path ./metro-proto/metro/proto
	# Generate static assets for OpenAPI UI
	@statik -m -f -src third_party/OpenAPI/

.PHONY: proto-clean ## Clean generated proto files
proto-clean:
	@rm -rf $(RPC_ROOT)

.PHONY: proto-refresh ## Re-compile protobuf
proto-refresh: clean proto-generate

.PHONY: build-info
build-info:
	@echo "\nBuild Info:\n"
	@echo "\t\033[33mOS\033[0m: $(UNAME_OS)"
	@echo "\t\033[33mArch\033[0m: $(UNAME_ARCH)"
	@echo "\t\033[33mGo Version\033[0m: $(GOVERSION)\n"

.PHONY: go-build-api ## Build the binary file for API server
go-build-metro:
	@CGO_ENABLED=1 GOOS=$(UNAME_OS) GOARCH=$(UNAME_ARCH) go build -tags musl -v -o $(METRO_OUT) $(METRO_MAIN_FILE)

.PHONY: clean ## Remove mocks, previous builds, protobuf files, and proto compiled code
clean: mock-gen-clean proto-clean
	@echo " + Removing cloned and generated files\n"
	##- todo: use go clean here
	@rm -rf $(METRO_OUT) $(TMP_DIR)/* $(DOCS_DIR)/$(UML_OUT_FILE)

.PHONY: dev-docker-up ## Bring up docker-compose for local dev-setup
dev-docker-up:
	docker-compose -f deployment/dev/monitoring/docker-compose.yml up -d
	docker-compose -f deployment/dev/docker-compose.yml up -d --build

.PHONY: dev-docker-datastores-up ## Bring up datastore containers
dev-docker-datastores-up:
	docker-compose -f deployment/dev/docker-compose-datastores.yml up -d

.PHONY: dev-docker-datastores-down ## Shut down datastore containers
dev-docker-datastores-down:
	docker-compose -f deployment/dev/docker-compose-datastores.yml down

.PHONY: dev-docker-down ## Shutdown docker-compose for local dev-setup
dev-docker-down:
	@docker-compose -f deployment/dev/docker-compose.yml down
	@docker-compose -f deployment/dev/monitoring/docker-compose.yml down

.PHONY: dev-docker-emulator-up ## Bring up google pub/sub emulator
dev-docker-emulator-up:
	docker-compose -f deployment/dev/docker-compose-emulator.yml up -d

.PHONY: dev-docker-emulator-down ## Bring down google pub/sub emulator
dev-docker-emulator-down:
	docker-compose -f deployment/dev/docker-compose-emulator.yml down

.PHONY: docker-build-metro
docker-build-metro:
	@docker build . -f build/docker/Dockerfile --build-arg GIT_TOKEN=${GIT_TOKEN} -t razorpay/metro:latest

.PHONY: mock-gen ## Generates mocks
mock-gen:
	@go generate ./...
	@mockgen --build_flags='--tags=musl' -destination=internal/brokerstore/mocks/mock_brokerstore.go -package=mocks github.com/razorpay/metro/internal/brokerstore IBrokerStore
	@mockgen --build_flags='--tags=musl' -destination=pkg/messagebroker/mocks/mock_broker.go -package=mocks github.com/razorpay/metro/pkg/messagebroker Broker
	@mockgen --build_flags='--tags=musl' -destination=pkg/messagebroker/mocks/mock_admin.go -package=mocks github.com/razorpay/metro/pkg/messagebroker Admin
	@mockgen --build_flags='--tags=musl' -destination=internal/topic/mocks/core/mock_core.go -package=mocks github.com/razorpay/metro/internal/topic ICore
	@mockgen --build_flags='--tags=musl' -destination=internal/topic/mocks/repo/mock_repo.go -package=mocks github.com/razorpay/metro/internal/topic IRepo
	@mockgen --build_flags='--tags=musl' -destination=internal/subscription/mocks/core/mock_core.go -package=mocks github.com/razorpay/metro/internal/subscription ICore
	@mockgen --build_flags='--tags=musl' -destination=internal/subscription/mocks/repo/mock_repo.go -package=mocks github.com/razorpay/metro/internal/subscription IRepo
	@mockgen --build_flags='--tags=musl' -destination=internal/project/mocks/core/mock_core.go -package=mocks github.com/razorpay/metro/internal/project ICore
	@mockgen --build_flags='--tags=musl' -destination=internal/project/mocks/repo/mock_repo.go -package=mocks github.com/razorpay/metro/internal/project IRepo
	@mockgen --build_flags='--tags=musl' -destination=internal/node/mocks/core/mock_core.go -package=mocks github.com/razorpay/metro/internal/node ICore
	@mockgen --build_flags='--tags=musl' -destination=internal/node/mocks/repo/mock_repo.go -package=mocks github.com/razorpay/metro/internal/node IRepo
	@mockgen --build_flags='--tags=musl' -destination=internal/nodebinding/mocks/core/mock_core.go -package=mocks github.com/razorpay/metro/internal/nodebinding ICore
	@mockgen --build_flags='--tags=musl' -destination=internal/nodebinding/mocks/repo/mock_repo.go -package=mocks github.com/razorpay/metro/internal/nodebinding IRepo
	@mockgen --build_flags='--tags=musl' -destination=internal/credentials/mocks/core/mock_core.go -package=mocks github.com/razorpay/metro/internal/credentials ICore
	@mockgen --build_flags='--tags=musl' -destination=internal/credentials/mocks/repo/mock_repo.go -package=mocks github.com/razorpay/metro/internal/credentials IRepo
	@mockgen --build_flags='--tags=musl' -destination=pkg/registry/mocks/mock_registry.go -package=mocks github.com/razorpay/metro/pkg/registry IRegistry
	@mockgen --build_flags='--tags=musl' -destination=pkg/registry/mocks/mock_watcher.go -package=mocks github.com/razorpay/metro/pkg/registry IWatcher
	@mockgen --build_flags='--tags=musl' -destination=internal/publisher/mocks/core/mock_core.go -package=mocks github.com/razorpay/metro/internal/publisher IPublisher
	@mockgen --build_flags='--tags=musl' -destination=service/web/stream/mocks/manager/mock_manager.go -package=mocks github.com/razorpay/metro/service/web/stream IManager

.PHONY: mock-gen-clean ## Clean up all mockgen generated mocks directories
mock-gen-clean:
	@find . -type d -name 'mocks' | xargs rm -rf

.PHONY: docs-uml ## Generates UML file
docs-uml:
	@go-plantuml generate --recursive --directories cmd --directories internal --directories pkg --out $(DOCS_DIR)/$(UML_OUT_FILE)

docs-gh:
	ruby -v
	bundle -v
	cd docs; bundle install
	cd docs; bundle exec jekyll serve

.PHONY: test-functional-ci ## run functional tests on ci (github actions)
test-functional-ci:
	@METRO_TEST_HOST=metro-web MOCK_SERVER_HOST=mock-server go test ./tests/functional/... -tags=functional,musl

.PHONY: test-functional ## run integration tests locally (metro service needs to be up)
test-functional:
	@METRO_TEST_HOST=localhost MOCK_SERVER_HOST=localhost go test --count=1 ./tests/functional/... -tags=functional,musl

.PHONY: test-integration-ci ## run integration tests on ci (github actions)
test-integration-ci: test-unit-prepare
	@METRO_TEST_HOST=metro-web KAFKA_TEST_HOST=kafka-broker go test -tags=integration,musl -timeout 2m -coverpkg=$(shell comm -23 $(TMP_DIR)/$(PKG_LIST_TMP_FILE) $(UNIT_TEST_EXCLUSIONS_FILE) | xargs | sed -e 's/ /,/g') -coverprofile=$(TMP_DIR)/$(INTG_COVERAGE_TMP_FILE) ./tests/integration/...

.PHONY: test-integration ## run integration tests locally (metro service needs to be up)
test-integration: test-unit-prepare
<<<<<<< Updated upstream
	@METRO_TEST_HOST=localhost KAFKA_TEST_HOST=localhost go test --count=1 -tags=integration,musl -coverprofile=$(TMP_DIR)/$(INTG_COVERAGE_TMP_FILE) ./tests/integration/...
=======
    @METRO_TEST_HOST=localhost KAFKA_TEST_HOST=localhost go test --count=1 -tags=integration,musl -timeout 2m -coverpkg=$(shell comm -23 $(TMP_DIR)/$(PKG_LIST_TMP_FILE) $(UNIT_TEST_EXCLUSIONS_FILE) | xargs | sed -e 's/ /,/g')  -coverprofile=$(TMP_DIR)/$(INTG_COVERAGE_TMP_FILE) ./tests/integration/...
>>>>>>> Stashed changes

.PHONY: test-compat-ci ## run compatibility tests on ci (github actions)
test-compat-ci:
	@METRO_TEST_HOST=metro-web PUBSUB_TEST_HOST=pubsub go test -v ./tests/compatibility/... -tags=compatibility,musl

.PHONY: test-compat ## run compatibility tests locally (metro service and pubsub emulator needs to be up)
test-compat:
	@METRO_TEST_HOST=localhost PUBSUB_TEST_HOST=localhost go test --count=1 -v ./tests/compatibility/... -tags=compatibility,musl

.PHONY: test-unit-prepare
test-unit-prepare:
	@mkdir -p $(TMP_DIR)
	@go list ./... | grep -Ev 'tests|mocks|statik|rpc' > $(TMP_DIR)/$(PKG_LIST_TMP_FILE)

.PHONY: test-unit ## Run unit tests
test-unit: test-unit-prepare
	@APP_ENV=dev_docker go test --count=1 -tags=unit,musl -timeout 2m -coverpkg=$(shell comm -23 $(TMP_DIR)/$(PKG_LIST_TMP_FILE) $(UNIT_TEST_EXCLUSIONS_FILE) | xargs | sed -e 's/ /,/g') -coverprofile=$(TMP_DIR)/$(UNIT_COVERAGE_TMP_FILE) ./...
	@go tool cover -func=$(TMP_DIR)/$(UNIT_COVERAGE_TMP_FILE)

.PHONY: help ## Display this help screen
help:
	@echo "Usage:"
	@grep -E '^\.PHONY: [a-zA-Z_-]+.*?## .*$$' $(MAKEFILE_LIST) | sort | sed 's/\.PHONY\: //' | awk 'BEGIN {FS = " ## "}; {printf "\t\033[36m%-30s\033[0m %s\n", $$1, $$2}'
