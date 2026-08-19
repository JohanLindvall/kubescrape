package tailer

// Regressions for the node-metadata half of a file's resource: it is rendered
// from whatever the provider yields NOW, not latched at first resolve.

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
)

// zoneBuilder is the operator template from attrs.Config's own doc comment and
// docs/CONFIGURATION.md — the documented way to lift a node label onto every
// record, and the thing a latched resource silently drops.
func zoneBuilder(t *testing.T) *attrs.Builder {
	t.Helper()
	b, err := attrs.NewBuilder(&attrs.Config{
		Attributes: map[string]string{
			"k8s.node.zone": `{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}`,
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// nodeProvider is the shipped wiring's shape: selfmeta.Poll hands back a seeded
// placeholder (the bare node name, no labels) synchronously and swaps in a
// freshly allocated value on every successful resolve.
type nodeProvider struct {
	v atomic.Pointer[attrs.NodeInfo]
}

func (p *nodeProvider) set(labels map[string]string) {
	p.v.Store(&attrs.NodeInfo{Name: "node1", Labels: labels})
}

func (p *nodeProvider) get() *attrs.NodeInfo { return p.v.Load() }

// lastZone returns the k8s.node.zone of the most recently exported batch.
func lastZone(exp *fakeExporter) any {
	exp.mu.Lock()
	defer exp.mu.Unlock()
	return exp.resAttrs["k8s.node.zone"]
}

// TestPlaceholderNodeMetadataDoesNotLatchAResolvedFile pins the fix for a file
// that resolves in the window before the first GET /v1/nodes/{name}/metadata
// lands. The shipped agent seeds selfmeta.Poll with a non-nil placeholder, so
// that window is not hypothetical — it is every file discovered before the
// first fetch, and a latched resource kept the label-less shape for the whole
// life of the file while files discovered a second later carried the real one.
func TestPlaceholderNodeMetadataDoesNotLatchAResolvedFile(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	prov := &nodeProvider{}
	prov.set(nil) // the placeholder: name only, no labels

	tl := New(Config{
		Dir:           dir,
		PollInterval:  20 * time.Millisecond,
		FlushInterval: time.Millisecond,
		BatchSize:     1 << 20,
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		Attrs:         zoneBuilder(t),
		NodeInfo:      prov.get,
		Exporter:      exp,
	})
	tl.retryBackoff = time.Millisecond
	tl.scanDir(tl.loadCheckpoints(), true)

	writeLog(t, dir, timeNowCRI()+" stdout F early")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	if got := lastZone(exp); got != nil {
		t.Fatalf("setup: expected no zone before the node fetch landed, got %v", got)
	}

	// The node fetch lands.
	prov.set(map[string]string{"topology.kubernetes.io/zone": "eu-1a"})

	writeLog(t, dir, timeNowCRI()+" stdout F late")
	driveUntil(t, ctx, tl, func() bool {
		for _, r := range exp.get() {
			if r == "late" {
				return true
			}
		}
		return false
	}, "the second line of the SAME file to export")

	if got := lastZone(exp); got != "eu-1a" {
		t.Fatalf("the file resolved before the node metadata landed still exports zone=%v; "+
			"one node would export two resource shapes for the rest of the process's life", got)
	}
}

// TestNodeRelabelReachesAnAlreadyResolvedPlainFile needs no race at all: it is
// -node-metadata-refresh doing the job it is documented to do (".Node | ...
// refreshed per -node-metadata-refresh") for a file that was already tailing.
// Plain sources are the sharp case — they take no metadata lookup, so they
// resolve on sweep #1 and used to latch whatever the provider held then.
func TestNodeRelabelReachesAnAlreadyResolvedPlainFile(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	prov := &nodeProvider{}
	prov.set(map[string]string{"topology.kubernetes.io/zone": "eu-1a"})

	tl := New(Config{
		Sources: []Source{{
			Name:    "host",
			Include: []string{filepath.Join(dir, "*.log")},
		}},
		PollInterval:  20 * time.Millisecond,
		FlushInterval: time.Millisecond,
		BatchSize:     1 << 20,
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		Attrs:         zoneBuilder(t),
		NodeInfo:      prov.get,
		Exporter:      exp,
	})
	tl.retryBackoff = time.Millisecond
	tl.scanDir(tl.loadCheckpoints(), true)

	path := filepath.Join(dir, "host.log")
	writeLines(t, path, "before relabel")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool {
		for _, r := range exp.get() {
			if r == "before relabel" {
				return true
			}
		}
		return false
	}, "the first line to export")
	if got := lastZone(exp); got != "eu-1a" {
		t.Fatalf("setup: want zone eu-1a on the first export, got %v", got)
	}

	// The node is relabelled and the provider refreshes.
	prov.set(map[string]string{"topology.kubernetes.io/zone": "eu-1b"})

	writeLines(t, path, "after relabel")
	driveUntil(t, ctx, tl, func() bool {
		for _, r := range exp.get() {
			if r == "after relabel" {
				return true
			}
		}
		return false
	}, "the post-relabel line to export")

	if got := lastZone(exp); got != "eu-1b" {
		t.Fatalf("node relabelled to eu-1b, an already-tailed file still exports zone=%v", got)
	}
}

// TestPodAnnotationAttributesSurviveANodeMetadataRefresh pins the seam the
// re-render opens: everything that shapes the resource must be applied by the
// renderer, not once beside the resolve. The pod annotation's overrides are
// the workload's own statement about itself and must outlive a node relabel.
func TestPodAnnotationAttributesSurviveANodeMetadataRefresh(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	prov := &nodeProvider{}
	prov.set(map[string]string{"topology.kubernetes.io/zone": "eu-1a"})

	tl := New(Config{
		Dir:           dir,
		PollInterval:  20 * time.Millisecond,
		FlushInterval: time.Millisecond,
		BatchSize:     1 << 20,
		MetadataWait:  time.Second,
		Metadata:      annotatedMeta{`{"serviceName":"checkout","attributes":{"team":"payments"}}`},
		Attrs:         zoneBuilder(t),
		NodeInfo:      prov.get,
		Exporter:      exp,
	})
	tl.retryBackoff = time.Millisecond
	tl.scanDir(tl.loadCheckpoints(), true)

	writeLog(t, dir, timeNowCRI()+" stdout F first")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool {
		for _, r := range exp.get() {
			if r == "first" {
				return true
			}
		}
		return false
	}, "the first line to export")

	prov.set(map[string]string{"topology.kubernetes.io/zone": "eu-1b"})
	writeLog(t, dir, timeNowCRI()+" stdout F second")
	driveUntil(t, ctx, tl, func() bool {
		for _, r := range exp.get() {
			if r == "second" {
				return true
			}
		}
		return false
	}, "the post-relabel line to export")

	exp.mu.Lock()
	got := exp.resAttrs
	exp.mu.Unlock()
	if got["k8s.node.zone"] != "eu-1b" {
		t.Fatalf("the re-render did not pick up the relabel: zone=%v", got["k8s.node.zone"])
	}
	if got["service.name"] != "checkout" || got["team"] != "payments" {
		t.Fatalf("the pod annotation's overrides did not survive the re-render: service.name=%v team=%v",
			got["service.name"], got["team"])
	}
}
