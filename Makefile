BINARY    := bitcoin-shard-listener
SINK      := sink-test-frames
VERSION   ?= $(shell git describe --tags --dirty 2>/dev/null || echo "dev")
TAG       ?= $(VERSION)
IMAGE     ?= ghcr.io/lightwebinc/$(BINARY)
COMMON    ?= ../bitcoin-shard-common
LDFLAGS   := -buildvcs=false -ldflags "-X github.com/lightwebinc/bitcoin-shard-listener/metrics.Version=$(VERSION)"
BUILD_DIR := build

PROXY_DIR := ../bitcoin-shard-proxy
PROXY_BIN := $(PROXY_DIR)/bitcoin-shard-proxy
SEND_BIN  := $(PROXY_DIR)/send-test-frames

DAGGER_RUN := GOWORK=off go run .

.PHONY: all build test test-e2e lint hooks clean docker FORCE \
        ci ci-unit ci-lint ci-vuln ci-tidy ci-build ci-image ci-export ci-publish ci-shell \
        fmt help

all: build

FORCE:

build:
	mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY) .

$(BINARY): FORCE
	go build -buildvcs=false -o $(BINARY) .

$(SINK): FORCE
	go build -buildvcs=false -o $(SINK) ./cmd/sink-test-frames/

$(PROXY_BIN): FORCE
	$(MAKE) -C $(PROXY_DIR) bitcoin-shard-proxy

$(SEND_BIN): FORCE
	(cd $(PROXY_DIR) && go build -buildvcs=false -o send-test-frames ./cmd/send-test-frames/)

test:
	go test -race ./...

test-e2e: $(BINARY) $(SINK) $(SEND_BIN)
	PATH="$(CURDIR):$(abspath $(PROXY_DIR)):$$PATH" sh test/run-e2e.sh

lint:
	golangci-lint run ./...

hooks:
	git config core.hooksPath .githooks
	@echo "pre-commit hook installed (git config core.hooksPath .githooks)"

clean:
	rm -rf $(BUILD_DIR) $(BINARY) $(SINK)

docker:
	docker build -t $(BINARY):$(VERSION) .

# --- Dagger CI (containerised, reproducible) ---

ci:                    ## full pipeline: tidy + lint + vuln + unit + build + image
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) -version=$(VERSION) all

ci-unit:               ## go test -race ./... inside Dagger
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) unit

ci-lint:               ## go vet + golangci-lint inside Dagger
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) lint

ci-vuln:               ## govulncheck inside Dagger
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) vuln

ci-tidy:               ## go mod tidy diff check inside Dagger
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) tidy

ci-build:              ## go build ./... inside Dagger (no image)
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) build

ci-image:              ## build OCI image (cached only)
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) -version=$(VERSION) image

ci-export:             ## export image to build/$(BINARY)-$(TAG).tar
	@mkdir -p build
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) -version=$(VERSION) \
	  -export=../build/$(BINARY)-$(TAG).tar image

ci-publish:            ## publish image to $(IMAGE):$(TAG)
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) -version=$(VERSION) \
	  -address=$(IMAGE):$(TAG) image

ci-shell:              ## interactive shell in the builder container
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) dev-shell

fmt:                   ## gofmt -w
	gofmt -w .

help:                  ## list targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort
