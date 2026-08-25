GO ?= go
DOCKER ?= docker
BUILD_DIR ?= .build
IMAGE ?= cwm-minio-tierer:local

.PHONY: all fmt fmt-check test race vet build validate compose-config image integration integration-ilm integration-clean

all: validate

fmt:
	$(GO) fmt ./...

fmt-check:
	@test -z "$$(gofmt -l cmd internal)" || { gofmt -l cmd internal; exit 1; }

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

build:
	mkdir -p "$(BUILD_DIR)"
	CGO_ENABLED=0 $(GO) build -buildvcs=false -mod=readonly -trimpath -o "$(BUILD_DIR)/redis-updater" ./cmd/redis-updater
	CGO_ENABLED=0 $(GO) build -buildvcs=false -mod=readonly -trimpath -o "$(BUILD_DIR)/minio-tierer" ./cmd/minio-tierer

compose-config:
	$(DOCKER) compose config --quiet

image:
	$(DOCKER) build --pull=false --target runtime -t "$(IMAGE)" .

validate: fmt-check test race vet build compose-config

integration:
	./integration/run.sh

integration-ilm:
	INTEGRATION_MINIO_ILM=true ./integration/run.sh

integration-clean:
	$(DOCKER) compose down --volumes --remove-orphans
