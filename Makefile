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
COVERAGE_TMP_FILE := "app.cov"
UNIT_TEST_EXCLUSIONS_FILE := "unit-test.exclusions"
# Proto gen info
PROTO_ROOT := "metro-proto/"
RPC_ROOT := "rpc/"

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

GOFILES     ?= $(shell find . -type f -name '*.go' -not -path "./vendor/*")

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
	@buf generate --path ./metro-proto/metro
	# Generate static assets for OpenAPI UI
	@statik -m -f -src third_party/OpenAPI/

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

.PHONY: clean ## Remove previous builds, protobuf files, and proto compiled code
clean:
	@echo " + Removing cloned and generated files\n"
	##- todo: use go clean here
	@rm -rf $(METRO_OUT) $(RPC_ROOT) $(TMP_DIR)/* $(DOCS_DIR)/$(UML_OUT_FILE)

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
	@docker-compose -f deployment/dev/docker-compose.yml down --remove-orphans
	@docker-compose -f deployment/dev/monitoring/docker-compose.yml down --remove-orphans

.PHONY: docker-build-metro
docker-build-metro:
	@docker build . -f build/docker/Dockerfile --build-arg GIT_TOKEN=${GIT_TOKEN} -t razorpay/metro:latest

.PHONY: mock-gen ## Generates mocks
mock-gen:
	## TODO: replace with go generate

.PHONY: docs-uml ## Generates UML file
docs-uml:
	@go-plantuml generate --recursive --directories cmd --directories internal --directories pkg --out $(DOCS_DIR)/$(UML_OUT_FILE)

.PHONY: test-unit-prepare
test-unit-prepare:
	@mkdir -p $(TMP_DIR)
	@go list ./... > $(TMP_DIR)/$(PKG_LIST_TMP_FILE)

.PHONY: test-unit ## Run unit tests
test-unit: test-unit-prepare
	@APP_ENV=dev_docker go test -tags=unit,musl -timeout 2m -coverpkg=$(shell comm -23 $(TMP_DIR)/$(PKG_LIST_TMP_FILE) $(UNIT_TEST_EXCLUSIONS_FILE) | xargs | sed -e 's/ /,/g') -coverprofile=$(TMP_DIR)/$(COVERAGE_TMP_FILE) ./...
	@go tool cover -func=$(TMP_DIR)/$(COVERAGE_TMP_FILE)

.PHONY: help ## Display this help screen
help:
	@echo "Usage:"
	@grep -E '^\.PHONY: [a-zA-Z_-]+.*?## .*$$' $(MAKEFILE_LIST) | sort | sed 's/\.PHONY\: //' | awk 'BEGIN {FS = " ## "}; {printf "\t\033[36m%-30s\033[0m %s\n", $$1, $$2}'
