package tailer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// annotatedMeta serves pod metadata carrying a kubescrape.io/logs annotation.
type annotatedMeta struct{ annotation string }

func (m annotatedMeta) Container(_ context.Context, id string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	return &kubemeta.ContainerMetadata{
		ContainerID: id,
		Container:   kubemeta.Container{Name: "app", ID: id},
		Pod: kubemeta.Pod{
			Name: "pod1", Namespace: "ns1", UID: "uid1", NodeName: "node1",
			Annotations: map[string]string{LogAnnotation: m.annotation},
		},
	}, nil
}

func TestPodAnnotationRulesAndAttributes(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Metadata = annotatedMeta{annotation: `{
		"serviceName": "checkout",
		"attributes": {"team": "payments"},
		"rules": [{"action": "drop", "matchRegexp": ["__line__=level=debug"]}]
	}`}

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F level=debug noisy line",
		"2026-07-05T10:00:01Z stdout F level=info kept line")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 1 }, "one kept record")

	if got := exp.get(); got[0] != "level=info kept line" {
		t.Fatalf("records = %v (pod drop rule ignored?)", got)
	}
	rec, ok := exp.record(0)
	if !ok {
		t.Fatal("no record")
	}
	_ = rec
	// Resource attrs: service.name overridden, team added.
	var f *file
	for _, ff := range tl.files {
		f = ff
	}
	if v, _ := f.resource.Attributes().Get("service.name"); v.Str() != "checkout" {
		t.Fatalf("service.name = %q, want checkout", v.Str())
	}
	if v, _ := f.resource.Attributes().Get("team"); v.Str() != "payments" {
		t.Fatalf("team = %q, want payments", v.Str())
	}
}

func TestPodAnnotationExclude(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Metadata = annotatedMeta{annotation: `{"exclude": true}`}

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F should never export")
	tl.scanDir(nil, false)
	for i := 0; i < 5; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("excluded pod exported: %v", got)
	}
	tl.publishStatus()
	if st := tl.Status(); len(st) != 0 {
		t.Fatalf("excluded file appears in status: %+v", st)
	}
}

func TestPodAnnotationMalformedIsIgnored(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Metadata = annotatedMeta{annotation: `{not json`}

	before := obs.LogPodConfigInvalid.Value()
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F still collected")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 1 }, "record despite bad annotation")
	if got := exp.get(); !strings.Contains(got[0], "still collected") {
		t.Fatalf("records = %v", got)
	}
	// Ignored is not silent: counted, and named on /debug/tailer.
	if got := obs.LogPodConfigInvalid.Value(); got != before+1 {
		t.Errorf("LogPodConfigInvalid = %v, want %v", got, before+1)
	}
	tl.publishStatus()
	st := tl.Status()
	if len(st) != 1 || st[0].PodConfigError == "" {
		t.Errorf("status must carry the parse error: %+v", st)
	}
}

// The multiline pod-annotation override must affect the file's INITIAL
// pipeline (it was wired at discovery, before the annotation was read),
// including for files that never rotate.
func TestPodAnnotationMultilineOverrideOnInitialPipeline(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	// Source default multiline OFF (driveTailer); the annotation turns it ON.
	tl := driveTailer(dir, exp)
	tl.cfg.MultilineTimeout = 50 * time.Millisecond
	tl.cfg.Metadata = annotatedMeta{annotation: `{"multiline": true}`}

	tl.scanDir(tl.loadCheckpoints(), true)
	start, rest := panicLines()
	writeLog(t, dir, append([]string{}, append(start, rest...)...)...)
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { j, _ := panicRecords(exp); return j == 1 }, "panic trace joined")

	// The stack-trace fragments joined into ONE record (main.go:10 present in
	// the same record as "panic: boom") — the override reached the initial
	// pipeline. Without the fix the trace stage was nil and they'd be split.
	joined, count := panicRecords(exp)
	if joined != 1 || count != 1 {
		t.Fatalf("multiline override not applied to initial pipeline: joined=%d count=%d", joined, count)
	}
}

// A pod annotation is authoritative about the workload's own DESCRIPTION, never
// about the identity the agent resolved from the API server. k8s.namespace.name
// in particular is what internal/agent/route keys tenancy on, so an unfiltered
// write let any user who can annotate a pod in their own namespace ship that
// pod's logs to another tenant's endpoint (and remove them from their own).
func TestPodAnnotationCannotForgeResolvedIdentity(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Metadata = annotatedMeta{annotation: `{
		"serviceName": "checkout",
		"attributes": {
			"team": "payments",
			"k8s.namespace.name": "victim-tenant",
			"k8s.pod.name": "someone-else",
			"k8s.node.name": "other-node",
			"container.id": "deadbeef",
			"service.instance.id": "forged",
			"service.namespace": "forged-ns"
		}
	}`}

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F hello")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 1 }, "one record")

	var f *file
	for _, ff := range tl.files {
		f = ff
	}
	attr := func(k string) string {
		v, _ := f.resource.Attributes().Get(k)
		return v.Str()
	}
	// The descriptive keys the annotation legitimately owns still apply.
	if got := attr("team"); got != "payments" {
		t.Errorf("descriptive attribute lost: team=%q, want payments", got)
	}
	if got := attr("service.name"); got != "checkout" {
		t.Errorf("serviceName override lost: service.name=%q, want checkout", got)
	}
	// The resolved identity is the agent's, and must survive verbatim.
	for _, c := range []struct{ key, want string }{
		{"k8s.namespace.name", "ns1"},
		{"k8s.pod.name", "pod1"},
		{"k8s.node.name", "node1"},
		{"service.namespace", "ns1"},
	} {
		if got := attr(c.key); got != c.want {
			t.Errorf("pod annotation forged %s=%q, want the resolved %q", c.key, got, c.want)
		}
	}
	if got := attr("service.instance.id"); got == "forged" {
		t.Error("pod annotation forged service.instance.id")
	}
	if got := attr("container.id"); got == "deadbeef" {
		t.Error("pod annotation forged container.id")
	}
}

// The per-pod rule bounds: these regexes run on the single sweep goroutine that
// serves every log file on the node, against every record including the whole
// raw body, and they come from an annotation any workload author controls.
func TestPodAnnotationRuleBounds(t *testing.T) {
	many := make([]string, 0, maxPodRules+1)
	for i := 0; i <= maxPodRules; i++ {
		many = append(many, `{"action":"drop","matchRegexp":["__line__=x"]}`)
	}
	for _, c := range []struct{ name, raw string }{
		{"too many rules", `{"rules":[` + strings.Join(many, ",") + `]}`},
		{"too many selectors", `{"rules":[{"action":"drop","matchRegexp":[` +
			strings.TrimSuffix(strings.Repeat(`"__line__=x",`, maxPodSelectors+1), ",") + `]}]}`},
		{"pattern too long", `{"rules":[{"action":"drop","matchRegexp":["__line__=` +
			strings.Repeat("a", maxPodPatternLength+1) + `"]}]}`},
	} {
		if _, _, err := parsePodLogConfig(c.raw); err == nil {
			t.Errorf("%s: accepted, want a bound error", c.name)
		}
	}
	// A reasonable annotation still compiles.
	if _, _, err := parsePodLogConfig(`{"rules":[{"action":"drop","matchRegexp":["__line__=debug"]}]}`); err != nil {
		t.Errorf("ordinary pod rules refused: %v", err)
	}
}
