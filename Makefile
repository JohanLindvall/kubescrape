BINARY  := kubescrape
IMAGE   ?= ghcr.io/johanlindvall/kubescrape
TAG     ?= latest
GOFLAGS := -trimpath

GOLANGCI_LINT_VERSION := v2.12.2
GOLANGCI_LINT         := $(shell go env GOPATH)/bin/golangci-lint

.PHONY: all build test vet fmt tidy lint run image cluster-up cluster-down clean

all: build

build:
	CGO_ENABLED=0 go build $(GOFLAGS) -o bin/$(BINARY) ./cmd/kubescrape
	CGO_ENABLED=1 go build $(GOFLAGS) -o bin/$(BINARY)-agent ./cmd/kubescrape-agent

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

tidy:
	go mod tidy

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run

$(GOLANGCI_LINT):
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

run: build
	./bin/$(BINARY)

image:
	docker build -t $(IMAGE):$(TAG) .

# Three-node kind test cluster (see hack/).
cluster-up:
	./hack/cluster-up.sh

cluster-down:
	./hack/cluster-down.sh

clean:
	rm -rf bin
