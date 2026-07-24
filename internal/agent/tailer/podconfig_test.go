package tailer

import (
	"context"
	"strings"
	"testing"
	"time"

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

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F still collected")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 1 }, "record despite bad annotation")
	if got := exp.get(); !strings.Contains(got[0], "still collected") {
		t.Fatalf("records = %v", got)
	}
}
