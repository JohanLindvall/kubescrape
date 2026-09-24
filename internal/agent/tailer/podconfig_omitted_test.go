package tailer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// filteredMeta serves pod metadata whose annotations went through the metadata
// service's REAL filter (kubemeta.FilterAnnotations), as every served pod does.
type filteredMeta struct{ annotations map[string]string }

func (m filteredMeta) Container(_ context.Context, id string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	return &kubemeta.ContainerMetadata{
		ContainerID: id,
		Container:   kubemeta.Container{Name: "app", ID: id},
		Pod: kubemeta.Pod{
			Name: "pod1", Namespace: "ns1", UID: "uid1", NodeName: "node1",
			Annotations: kubemeta.FilterAnnotations(m.annotations),
		},
	}, nil
}

// A kubescrape.io/logs value over the metadata service's per-annotation ceiling
// is omitted WHOLE from the pod document, so the agent receives no such key.
// Read as "no annotation", the pod's opt-out (or drop rules, or attributes) was
// silently discarded and the pod collected with the source defaults, while the
// only evidence sat on the metadata service. It must take the malformed path
// instead: logs still flow, and the refusal is counted and named on
// /debug/tailer on the node whose behaviour it changed.
func TestAnOmittedPodLogAnnotationIsReportedNotSilentlyIgnored(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Metadata = filteredMeta{annotations: map[string]string{
		LogAnnotation: `{"exclude": true, "attributes": {"pad": "` + strings.Repeat("x", kubemeta.MaxAnnotationValueBytes) + `"}}`,
	}}

	before := obs.LogPodConfigInvalid.Value()
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F still collected")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 1 }, "record despite the omitted annotation")
	if got := exp.get(); !strings.Contains(got[0], "still collected") {
		t.Fatalf("records = %v", got)
	}
	if got := obs.LogPodConfigInvalid.Value(); got != before+1 {
		t.Errorf("LogPodConfigInvalid = %v, want %v: an annotation the metadata service refused was read as absent", got, before+1)
	}
	tl.publishStatus()
	st := tl.Status()
	if len(st) != 1 || !strings.Contains(st[0].PodConfigError, "omitted") {
		t.Errorf("status must say the annotation was omitted: %+v", st)
	}
}

// The other side of the same check: a pod with NO log annotation — including
// one whose OTHER annotations were refused for size — is simply unconfigured.
// Nothing is counted and nothing is reported.
func TestAPodWithoutALogAnnotationIsNotReported(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Metadata = filteredMeta{annotations: map[string]string{
		"team.example.com/blob": strings.Repeat("x", kubemeta.MaxAnnotationValueBytes+1),
	}}

	before := obs.LogPodConfigInvalid.Value()
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F collected")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 1 }, "record from an unconfigured pod")
	if got := obs.LogPodConfigInvalid.Value(); got != before {
		t.Errorf("LogPodConfigInvalid moved %v for a pod with no log annotation", got-before)
	}
	tl.publishStatus()
	if st := tl.Status(); len(st) != 1 || st[0].PodConfigError != "" {
		t.Errorf("an unconfigured pod reported a pod-config error: %+v", st)
	}
}
