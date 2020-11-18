#############################################################
#
#  Razorpay (c) 2019
#
#  Leverages https://github.com/uber/prototool (MIT License)
#
#############################################################

SHELL := /bin/bash -o pipefail

UNAME_OS := $(shell uname -s)
UNAME_ARCH := $(shell uname -m)

TMP_BASE := .tmp
TMP := $(TMP_BASE)/$(UNAME_OS)/$(UNAME_ARCH)
TMP_BIN = $(TMP)/bin
TMP_VERSIONS := $(TMP)/versions

export GO111MODULE := on
export GOBIN := $(abspath $(TMP_BIN))
export PATH := $(GOBIN):$(PATH)

# This is the only variable that ever should change.
# This can be a branch, tag, or commit.
# When changed, the given version of Prototool will be installed to
# .tmp/$(uname -s)/(uname -m)/bin/prototool
PROTOTOOL_VERSION := v1.9.0

PROTOTOOL := $(TMP_VERSIONS)/prototool/$(PROTOTOOL_VERSION)
$(PROTOTOOL):
	$(eval PROTOTOOL_TMP := $(shell mktemp -d))
	cd $(PROTOTOOL_TMP); go get github.com/uber/prototool/cmd/prototool@$(PROTOTOOL_VERSION)
	@rm -rf $(PROTOTOOL_TMP)
	@rm -rf $(dir $(PROTOTOOL))
	@mkdir -p $(dir $(PROTOTOOL))
	@touch $(PROTOTOOL)

# proto is a target that uses prototool.
# By depending on $(PROTOTOOL), prototool will be installed on the Makefile's path.
# Since the path above has the temporary GOBIN at the front, this will use the
# locally installed prototool.

.PHONY: lint format fix

# make lint PACKAGE=stork/message/v1/message.proto
lint: $(PROTOTOOL) ## Run linter on a given package. Uses prototool.yaml file located in the package root.
	prototool lint $(PACKAGE)

# make format PACKAGE=stork/message/v1/message.proto
format: $(PROTOTOOL) ## Format a Protobuf file and print the formatted file to stdout
	prototool format $(PACKAGE)

# make fix PACKAGE=stork/message/v1/message.proto
fix: $(PROTOTOOL) ## Fix the file according to the Style Guide
	prototool format -f -w $(PACKAGE)

# make create PACKAGE=stork/message/v1/message.proto
create: $(PROTOTOOL) ## Create Protobuf files from a template
	prototool create $(PACKAGE)

# make generate PACKAGE=stork/message/v1/message.proto
generate: $(PROTOTOOL) ## Generate .pb.go files from Protobuf files
	prototool generate $(PACKAGE)

help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

## TODO: Add more prototool commands (generate, init, etc)
