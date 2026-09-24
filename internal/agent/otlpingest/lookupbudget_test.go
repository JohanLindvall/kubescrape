package otlpingest

// Regression tests bounding the serial in-handler metadata lookups one push
// may trigger (maxLookupsPerRequest) and pinning that resolvability PROBES
// never carry the configured wait: the metadata service parks each waited
// container lookup in its waiter map for the full -ingest-metadata-wait, so a
// push of invented ids used to hold an in-flight slot here AND a waiter slot
// there, once per distinct id, from the unauthenticated listener.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// recordingMeta counts every lookup and records each container lookup's wait.
type recordingMeta struct {
	*fakeMeta
	mu    sync.Mutex
	calls int
	waits map[string][]time.Duration
}

func (r *recordingMeta) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	r.mu.Lock()
	r.calls++
	if r.waits == nil {
		r.waits = map[string][]time.Duration{}
	}
	r.waits[id] = append(r.waits[id], wait)
	r.mu.Unlock()
	return r.fakeMeta.Container(ctx, id, wait)
}

func (r *recordingMeta) PodByUID(ctx context.Context, uid string) (*kubemeta.Pod, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	return r.fakeMeta.PodByUID(ctx, uid)
}

// Auto mode's foreign-point walk probes every distinct data-point id, an
// unresolvable id is "not foreign" so the walk continues, and metaclient never
// caches a 404 — one live GET per invented id, unbounded, each carrying the
// configured wait.
func TestAutoModeProbeLookupsAreBoundedAndWaitFree(t *testing.T) {
	meta := &recordingMeta{fakeMeta: newMeta()}
	e := NewEnricher(Config{Meta: meta, MetricsMode: MetricsAuto, Wait: 3 * time.Second})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.id", "cafe01")
	dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints()
	ids := maxLookupsPerRequest + 100
	for i := range ids {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("container.id", fmt.Sprintf("bogus-%d", i))
	}

	e.EnrichMetrics(context.Background(), md)

	if meta.calls > maxLookupsPerRequest {
		t.Errorf("lookups = %d, want <= %d: distinct-id lookups must be bounded per push", meta.calls, maxLookupsPerRequest)
	}
	for id, waits := range meta.waits {
		if id == "cafe01" {
			continue // the sender's own attribution may carry the wait
		}
		for _, w := range waits {
			if w != 0 {
				t.Fatalf("probe for %s carried wait %v; a probe must never be parked in the service's waiter map", id, w)
			}
		}
	}
}

// Split mode with BOTH id kinds on every point resolves the container id per
// distinct pair (resolvableToken's lookup, the one every mode shares) before the
// group cap can short-circuit, then builds the admitted groups' attributions;
// all of it draws on the one request budget, so the total stays bounded
// whatever the payload's shape.
func TestSplitModeLookupsAreBoundedPerPush(t *testing.T) {
	meta := &recordingMeta{fakeMeta: &fakeMeta{}}
	e := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint})

	md := pmetric.NewMetrics()
	dps := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty().SetEmptyGauge().DataPoints()
	// Sized so probes alone stay under the budget but probes + builds do not.
	points := maxLookupsPerRequest - maxSplitGroups + 1000
	for i := range points {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("container.id", fmt.Sprintf("c-%d", i))
		dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("u-%d", i))
	}

	out := e.EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != points {
		t.Fatalf("forwarded %d points, want all %d", got, points)
	}
	if meta.calls > maxLookupsPerRequest {
		t.Errorf("lookups = %d, want <= %d", meta.calls, maxLookupsPerRequest)
	}
	for id, waits := range meta.waits {
		for _, w := range waits {
			if w != 0 {
				t.Fatalf("split-path lookup for %s carried wait %v with no -ingest-metadata-wait configured, want 0", id, w)
			}
		}
	}
}

// A sender-supplied id has no length bound on the wire, and a lookup for it is
// a URL path segment the metadata service refuses past its 8 KiB header bound —
// while the failure is logged with that URL in it. An over-long id is therefore
// unresolvable without a lookup: no request, no budget spent, no log line
// carrying megabytes of it; the resource is forwarded unenriched and counted.
func TestOverlongLookupIDIssuesNoMetadataRequest(t *testing.T) {
	meta := &recordingMeta{fakeMeta: newMeta()}
	log, logged := capturedLogger()
	e := NewEnricher(Config{Meta: meta, MetricsMode: MetricsAuto, Logger: log})

	huge := strings.Repeat("a", 2<<20)
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().Resource().Attributes().PutStr("container.id", huge)
	pod := ld.ResourceLogs().AppendEmpty()
	pod.Resource().Attributes().PutStr("k8s.pod.uid", huge)
	// An ordinary id in the same push still resolves: the bound refuses the one
	// id, not the request.
	ld.ResourceLogs().AppendEmpty().Resource().Attributes().PutStr("container.id", "cafe01")
	before := obs.Ingested.WithLabelValues("unresolved").Value()

	e.EnrichLogs(context.Background(), ld)

	if meta.calls != 1 {
		t.Errorf("metadata lookups = %d, want 1 (only the ordinary id)", meta.calls)
	}
	if got := obs.Ingested.WithLabelValues("unresolved").Value() - before; got != 2 {
		t.Errorf("unresolved moved %v, want 2", got)
	}
	if v, _ := ld.ResourceLogs().At(2).Resource().Attributes().Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("the ordinary id in the same push did not resolve: k8s.pod.name = %q", v.Str())
	}
	if n := len(logged()); n > 64<<10 {
		t.Errorf("the refusal logged %d bytes", n)
	}
}
