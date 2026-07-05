BINARY  := kubescrape
IMAGE   ?= ghcr.io/johanlindvall/kubescrape
TAG     ?= latest
GOFLAGS := -trimpath

.PHONY: all build test vet fmt tidy run image cluster-up cluster-down clean

all: build

build:
	go build $(GOFLAGS) -o bin/$(BINARY) ./cmd/kubescrape
	go build $(GOFLAGS) -o bin/$(BINARY)-agent ./cmd/kubescrape-agent

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

tidy:
	go mod tidy

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
