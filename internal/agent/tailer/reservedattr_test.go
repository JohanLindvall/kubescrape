package tailer

import (
	"context"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// annMeta returns pod metadata carrying a kubescrape.io/logs annotation.
type annMeta struct{ ann string }

func (m annMeta) Container(_ context.Context, id string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	return &kubemeta.ContainerMetadata{
		ContainerID: id,
		Container:   kubemeta.Container{Name: "app", ID: id},
		Pod: kubemeta.Pod{
			Name: "pod1", Namespace: "tenant-a", UID: "uid1", NodeName: "node1",
			Annotations: map[string]string{LogAnnotation: m.ann},
		},
	}, nil
}

// A pod annotation may set descriptive attributes about itself, but NEVER
// kubescrape's control-plane markers (route.ScriptMarker / transform.DropMarker):
// the router honours the route marker before its namespace globs, so an
// unfiltered write would steer a tenant's own logs to another route and its
// tenant headers. Regression test for the reserved-plumbing gap.
func TestPodAnnotationCannotSetReservedPlumbing(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	ann := `{"attributes":{"` + route.ScriptMarker + `":"tenant-b","` +
		transform.DropMarker + `":"1","team":"payments"}}`
	tl := New(Config{
		Dir:           dir,
		PollInterval:  20 * time.Millisecond,
		FlushInterval: time.Millisecond,
		BatchSize:     1 << 20,
		MetadataWait:  time.Second,
		Metadata:      annMeta{ann: ann},
		Exporter:      exp,
	})
	tl.retryBackoff = time.Millisecond
	// The refusal is COUNTED per refused key, plumbing markers included —
	// kubescrape_log_pod_attrs_refused_total's help names both reserved sets,
	// and a counter that moved only for identity keys would hide the direct
	// form of the routing attack.
	routeBefore := obs.LogPodAttrsRefused.WithLabelValues(route.ScriptMarker).Value()
	dropBefore := obs.LogPodAttrsRefused.WithLabelValues(transform.DropMarker).Value()
	tl.scanDir(nil, true)
	writeLog(t, dir, timeNowCRI()+" stdout F hello")
	tl.scanDir(nil, false)
	ctx := context.Background()
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) > 0 }, "a record to be exported")

	if got := obs.LogPodAttrsRefused.WithLabelValues(route.ScriptMarker).Value() - routeBefore; got != 1 {
		t.Errorf("refused %q counted %v times, want 1", route.ScriptMarker, got)
	}
	if got := obs.LogPodAttrsRefused.WithLabelValues(transform.DropMarker).Value() - dropBefore; got != 1 {
		t.Errorf("refused %q counted %v times, want 1", transform.DropMarker, got)
	}

	exp.mu.Lock()
	ra := exp.resAttrs
	exp.mu.Unlock()
	if _, bad := ra[route.ScriptMarker]; bad {
		t.Errorf("pod annotation set the reserved route marker %q on the resource: %v", route.ScriptMarker, ra[route.ScriptMarker])
	}
	if _, bad := ra[transform.DropMarker]; bad {
		t.Errorf("pod annotation set the reserved drop marker %q on the resource", transform.DropMarker)
	}
	if got := ra["team"]; got != "payments" {
		t.Errorf("a legitimate descriptive attribute was dropped: team=%v (want payments)", got)
	}
}
