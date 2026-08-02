BINARY  := kubescrape
IMAGE   ?= ghcr.io/johanlindvall/kubescrape
TAG     ?= latest
GOFLAGS := -trimpath

# Optional pipelines are behind POSITIVE build tags, and THIS variable is what
# makes the shipped binaries contain them: `make build`, `make test`, `make
# vet`, `make lint` and `make image` all pass -tags $(TAGS), so the default
# below reproduces exactly the binaries and image this repo has always built.
#
#   journald  the systemd journal reader. It is the SOLE reason the agent needs
#             cgo: it links libsystemd through coreos/go-systemd/sdjournal, and
#             the image carries libsystemd + six transitive .so files on
#             distroless/base for it. Drop the tag and the agent is CGO-free
#             and fully static (see Dockerfile.static / `make image-static`).
#   azure     the Azure diagnostics consumer (Event Hubs over Kafka). Eleven
#             franz-go packages, in every DaemonSet image, for a feature that
#             only ever runs in the one-replica singleton Deployment.
#
#   make build                 # both, i.e. today's binaries
#   make build TAGS=azure      # no journald: CGO_ENABLED=0, static agent
#   make build TAGS=journald   # no franz-go
#   make build TAGS=           # neither
#
# NOTE: a bare `go build ./cmd/kubescrape-agent/` passes no tags and therefore
# produces an agent with NEITHER pipeline. `make build` is the supported path.
# Such a binary still DEFINES -journald and -azure-diagnostics (the manifests
# pass them and internal/manifestcheck guards that), but enabling one is a
# startup error naming the tag — and -check-config reports it too, along with
# an "optionalPipelines" field on the startup log line saying which binary you
# have.
TAGS     ?= journald,azure
TAGFLAGS := $(if $(strip $(TAGS)),-tags $(TAGS),)
# What `make image-static` builds: everything except journald, which is what
# makes both binaries static (see Dockerfile.static).
TAGS_STATIC ?= azure
# The agent needs cgo only for journald; without that tag it builds static.
AGENT_CGO := $(if $(findstring journald,$(TAGS)),1,0)

GOLANGCI_LINT_VERSION := v2.12.2
GOLANGCI_LINT         := $(shell go env GOPATH)/bin/golangci-lint

.PHONY: all build test vet fmt tidy lint run image image-static verify-tags cluster-up cluster-down clean

all: build

# Local build: unstripped, so delve/gdb work. The release IMAGE strips with
# -ldflags="-s -w" (see Dockerfile) for a ~30% smaller binary; that does not
# affect Go panic stack traces (the runtime reads pclntab, which -s/-w keep).
build:
	CGO_ENABLED=0 go build $(GOFLAGS) $(TAGFLAGS) -o bin/$(BINARY) ./cmd/kubescrape
	CGO_ENABLED=$(AGENT_CGO) go build $(GOFLAGS) $(TAGFLAGS) -o bin/$(BINARY)-agent ./cmd/kubescrape-agent

# The tags are passed here too, deliberately: without them the cmd tests would
# exercise the stub halves only, and every variant's tests would look green
# because the real code was never compiled. Test a variant the same way you
# build it (make test TAGS=azure).
test:
	go test $(TAGFLAGS) ./...

vet:
	go vet $(TAGFLAGS) ./...

fmt:
	gofmt -w .

tidy:
	go mod tidy

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run $(if $(strip $(TAGS)),--build-tags=$(TAGS),)

$(GOLANGCI_LINT):
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

# The guard for the build tags: that the agent WITHOUT journald really is
# CGO-free (a stray import of internal/agent/journald would fail this build,
# not merely re-add a dependency) and that the agent WITHOUT azure really has
# dropped franz-go. Cheap enough for CI.
verify-tags:
	CGO_ENABLED=0 go build $(GOFLAGS) -tags azure -o /dev/null ./cmd/kubescrape-agent
	@n=$$(go list -deps -tags journald ./cmd/kubescrape-agent | grep -c franz-go || true); \
		test "$$n" -eq 0 || { echo "franz-go still linked without the azure tag ($$n packages)"; exit 1; }
	@echo "build tags verified: no cgo without journald, no franz-go without azure"

run: build
	./bin/$(BINARY)

image:
	docker build --build-arg TAGS=$(TAGS) -t $(IMAGE):$(TAG) .

# Static variant: no journald, hence no cgo, hence no libsystemd — so both
# binaries fit distroless/static instead of distroless/base + seven .so files.
# The Azure consumer is kept (it is cgo-free); pass TAGS= to drop it too.
image-static:
	docker build -f Dockerfile.static --build-arg TAGS=$(TAGS_STATIC) -t $(IMAGE):$(TAG)-static .

# Three-node kind test cluster (see hack/).
cluster-up:
	./hack/cluster-up.sh

cluster-down:
	./hack/cluster-down.sh

clean:
	rm -rf bin
