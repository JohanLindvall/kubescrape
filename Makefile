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
#   events    the Kubernetes events reader and its leader election, which are
#             the ONLY reason the agent links k8s.io/client-go — 926 -> 470
#             dependency packages (k8s.io/ + sigs.k8s.io/ 412 -> 8) and MORE
#             THAN HALF the shipped binary (59.16 MB -> 26.27 MB, -55.6%), on
#             every node, for a pipeline that (like azure) only ever runs in
#             the one-replica singleton Deployment.
#
#   make build                 # all three, i.e. today's binaries
#   make build TAGS=azure,events   # no journald: CGO_ENABLED=0, static agent
#   make build TAGS=journald,events # no franz-go
#   make build TAGS=journald,azure  # no client-go: the smallest DaemonSet agent
#   make build TAGS=           # none of them
#
# NOTE: a bare `go build ./cmd/kubescrape-agent/` passes no tags and therefore
# produces an agent with NONE of them. `make build` is the supported path.
# Such a binary still DEFINES -journald, -azure-diagnostics and -events (the
# manifests pass them and internal/manifestcheck guards that), but enabling one is a
# startup error naming the tag — and -check-config reports it too, along with
# an "optionalPipelines" field on the startup log line saying which binary you
# have.
TAGS     ?= journald,azure,events
TAGFLAGS := $(if $(strip $(TAGS)),-tags $(TAGS),)
# What `make image-static` builds: everything except journald, which is what
# makes both binaries static (see Dockerfile.static).
TAGS_STATIC ?= azure,events
# The agent needs cgo only for journald; without that tag it builds static.
AGENT_CGO := $(if $(findstring journald,$(TAGS)),1,0)

GOLANGCI_LINT_VERSION := v2.12.2
GOLANGCI_LINT         := $(shell go env GOPATH)/bin/golangci-lint

.PHONY: all build test vet fmt fmt-check tidy lint run image image-static verify-tags helm-lint check cluster-up cluster-down e2e chaos clean

all: build

# Everything a pre-merge check runs, ordered to fail fastest: formatting
# (reported, not rewritten — `make fmt` fixes), then static analysis, then the
# chart (helm-lint also bootstraps helm into hack/bin, which the chart golden
# tests in internal/chartcheck pick up), then the test suite and the build-tag
# guard.
#
# What one green `make check` covers, stated exactly, because it was documented
# as "the whole local CI story" while a whole file went uncompiled: the DEFAULT
# tag set is vetted, linted and TESTED, and EVERY variant's stubs are COMPILED
# (verify-tags, below). What CI adds on top is the tag-less variant's TEST run
# (`make build test TAGS=`) and `CGO_ENABLED=1 go test -race` over the
# concurrency-touching packages — the latter already documented as a separate
# manual step in CLAUDE.md. `make e2e` needs docker/kind and is deliberately
# separate from both.
check: fmt-check vet lint helm-lint test verify-tags

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

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "needs gofmt (run make fmt):"; echo "$$out"; exit 1; fi

helm-lint:
	"$$(./hack/ensure-helm.sh)" lint charts/kubescrape

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
#
# The THIRD command is not about linking at all — it is the only step of `make
# check` that TYPE-CHECKS the stubs. `-tags azure` compiles journald_disabled.go
# (its `!journald` constraint holds), but the franz-go half is a `go list -deps`,
# which resolves imports WITHOUT type-checking, so azure_disabled.go — the repo's
# only `//go:build !azure` file — was compiled by no step of `make check` at all:
# a broken edit to it passed fmt-check, vet, lint and the whole test suite (its
# own build constraint excludes it under the default tags) and failed in CI's
# `make build test TAGS=`. A tag-less build compiles BOTH stubs, needs no cgo and
# no libsystemd, and costs a few seconds. Do not "simplify" it away as redundant
# with the -tags azure build above: they cover different files.
verify-tags:
	CGO_ENABLED=0 go build $(GOFLAGS) -tags azure,events -o /dev/null ./cmd/kubescrape-agent
	@n=$$(go list -deps -tags journald,events ./cmd/kubescrape-agent | grep -c franz-go || true); \
		test "$$n" -eq 0 || { echo "franz-go still linked without the azure tag ($$n packages)"; exit 1; }
	@n=$$(go list -deps -tags journald,azure ./cmd/kubescrape-agent | grep -cE '^(k8s\.io/client-go|k8s\.io/api/)' || true); \
		test "$$n" -eq 0 || { echo "k8s.io/client-go still linked without the events tag ($$n packages) — half the agent binary is riding on one import"; exit 1; }
	CGO_ENABLED=0 go build $(GOFLAGS) -o /dev/null ./cmd/kubescrape-agent
	@echo "build tags verified: no cgo without journald, no franz-go without azure, no client-go without events, every stub compiles"

run: build
	./bin/$(BINARY)

image:
	docker build --build-arg TAGS=$(TAGS) -t $(IMAGE):$(TAG) .

# Static variant: no journald, hence no cgo, hence no libsystemd — so both
# binaries fit distroless/static instead of distroless/base + seven .so files.
# The Azure consumer is kept (it is cgo-free); pass TAGS_STATIC= to drop it too.
#
# TAGS_STATIC, not TAGS: this target deliberately does not read TAGS, because
# TAGS defaults to journald,azure,events and journald is the whole reason the
# static image cannot exist. The comment used to say `TAGS=`, which silently did
# nothing and left franz-go in the image the operator was trying to slim.
image-static:
	docker build -f Dockerfile.static --build-arg TAGS=$(TAGS_STATIC) -t $(IMAGE):$(TAG)-static .

# Three-node kind test cluster (see hack/).
cluster-up:
	./hack/cluster-up.sh

cluster-down:
	./hack/cluster-down.sh

# End-to-end smoke test: build the image, load it into the kind cluster
# (created if absent), deploy the shipped manifests plus the debug collector,
# and assert the pipeline works — readiness gates clear, targets are
# discovered, the store answers, telemetry reaches the collector, and the
# protobuf scrape path converts a real native histogram (hack/nhexporter). The
# cluster is left running for iteration; KEEP=0 tears it down afterwards.
e2e:
	./hack/e2e.sh

# Chaos scenarios against the deployment `make e2e` leaves running. Each one
# breaks something an operator's cluster will break on its own — the collector
# goes away, the agent is SIGKILLed, logs rotate under load, the API server is
# blackholed — and asserts the invariant that must survive it. They take
# minutes each (real outages, real recovery), so they are NOT part of `make
# check`; run one at a time with hack/chaos/<name>.sh, or all of them here.
chaos:
	@for s in hack/chaos/collector-outage.sh hack/chaos/agent-kill.sh \
	          hack/chaos/log-rotation.sh hack/chaos/apiserver-blackhole.sh; do \
	  echo; echo "=== $$s ==="; "$$s" || exit 1; \
	done
	@echo; echo "all chaos scenarios PASSED"

clean:
	rm -rf bin
