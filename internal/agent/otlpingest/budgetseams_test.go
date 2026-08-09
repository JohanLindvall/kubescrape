package otlpingest

// Regression tests for the SEAMS between the per-request budgets: the lookup
// allowance the auto-mode decision spends on probes is also the allowance the
// sender's own attribution needs, and the decision's two halves cost wildly
// different amounts.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// A push whose data points name thousands of unresolvable ids must still
// attribute the SENDER. The auto-mode decision probes every distinct point id
// (an unresolvable one is deliberately not evidence of anything, so the walk
// does not stop), and probes used to charge the same allowance the resource's
// own attribution is paid from — so a resource carrying a perfectly resolvable
// container.id came out unenriched, counted unresolved, indistinguishable from
// an id the cluster never had. Any pod could do it to itself with one label.
func TestAttributionSurvivesABudgetExhaustingProbeWalk(t *testing.T) {
	meta := &recordingMeta{fakeMeta: newMeta()}
	e := NewEnricher(Config{Meta: meta, MetricsMode: MetricsAuto})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.id", "cafe01") // resolvable
	dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints()
	points := maxLookupsPerRequest + 10
	for i := 0; i < points; i++ {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("bogus-%d", i))
	}

	out := e.EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != points {
		t.Fatalf("forwarded %d points, want all %d", got, points)
	}
	a := out.ResourceMetrics().At(0).Resource().Attributes()
	if v, ok := a.Get("k8s.pod.name"); !ok || v.Str() != "web-1" {
		t.Errorf("k8s.pod.name = %q (present=%v), want web-1: the probe walk spent the sender's attribution budget", v.Str(), ok)
	}
	if meta.calls > maxLookupsPerRequest {
		t.Errorf("lookups = %d, want <= %d: the reserved share must not raise the total", meta.calls, maxLookupsPerRequest)
	}
}

// The decision's cheap half must run to completion before its expensive half
// starts: one resource with NO id at all settles the whole push, so no other
// resource's data points are worth walking — and that walk is what spends the
// lookup budget, on probes whose answer cannot change the outcome.
func TestResourceIDPassPrecedesThePointWalk(t *testing.T) {
	meta := &recordingMeta{fakeMeta: newMeta()}
	e := NewEnricher(Config{Meta: meta, MetricsMode: MetricsAuto, Wait: 3 * time.Second})

	md := pmetric.NewMetrics()
	rm0 := md.ResourceMetrics().AppendEmpty()
	rm0.Resource().Attributes().PutStr("container.id", "cafe01")
	dps := rm0.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints()
	for i := 0; i < 5000; i++ {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("container.id", fmt.Sprintf("bogus-%d", i))
	}
	// The decisive resource: no container id, no pod uid, so resource mode
	// cannot suffice whatever rm0's points say.
	rm1 := md.ResourceMetrics().AppendEmpty()
	rm1.Resource().Attributes().PutStr("service.name", "no-id-here")
	dp := rm1.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)

	e.EnrichMetrics(context.Background(), md)

	// A wait-free container lookup is a resolvability PROBE (attribution carries
	// Config.Wait), and no probe may be issued for a decision already made.
	for id, waits := range meta.waits {
		for _, w := range waits {
			if w == 0 {
				t.Fatalf("probed %s while a later resource already decided the answer", id)
			}
		}
	}
}

// blockingMeta parks every waited container lookup for its FULL requested
// wait — the metadata service's behaviour for an id that never appears — and
// records the waits it was granted, in order.
type blockingMeta struct {
	*fakeMeta
	mu    sync.Mutex
	waits []time.Duration
}

func (b *blockingMeta) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	b.mu.Lock()
	b.waits = append(b.waits, wait)
	b.mu.Unlock()
	if wait > 0 {
		time.Sleep(wait)
	}
	return b.fakeMeta.Container(ctx, id, wait)
}

// ghostLogs is one ResourceLogs per distinct never-to-resolve container id —
// the shape whose every attribution lookup parks for the full wait.
func ghostLogs(ids int) plog.Logs {
	ld := plog.NewLogs()
	for i := 0; i < ids; i++ {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("container.id", fmt.Sprintf("ghost-%d", i))
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	}
	return ld
}

// The count budget bounds how many lookups one push may issue; it never
// bounded TIME. With a non-default -ingest-metadata-wait each distinct
// fabricated id parked its attribution lookup for the full wait, serially,
// inside the handler — the in-flight slot (and, on HTTP, the byte-budget
// charge) held for count x wait, sender-controlled via the distinct-id count.
// The wait-time budget clamps the total: the granted waits can never sum past
// lookupWaitBudgetFactor x Wait (each waited call elapses at least what it was
// granted), later lookups run wait-free, and none of it costs the count
// budget a lookup.
func TestWaitBudgetBoundsARequestsBlockedTime(t *testing.T) {
	const wait = 25 * time.Millisecond
	const ids = 12
	meta := &blockingMeta{fakeMeta: newMeta()}
	e := NewEnricher(Config{Meta: meta, Wait: wait})

	start := time.Now()
	e.EnrichLogs(context.Background(), ghostLogs(ids))
	elapsed := time.Since(start)

	if len(meta.waits) != ids {
		t.Fatalf("lookups = %d, want %d: the wait budget must not spend the count budget", len(meta.waits), ids)
	}
	var granted time.Duration
	zeroed := 0
	for _, w := range meta.waits {
		granted += w
		if w == 0 {
			zeroed++
		}
	}
	if budget := lookupWaitBudgetFactor * wait; granted > budget {
		t.Errorf("granted waits sum to %v, want <= the %v budget", granted, budget)
	}
	if zeroed == 0 {
		t.Error("no lookup ran wait-free: the push should have exhausted the wait budget")
	}
	// Wall-clock sanity with generous slack: unbounded stacking is ids x wait.
	if limit := time.Duration(ids-2) * wait; elapsed >= limit {
		t.Errorf("the handler blocked %v; the budget bounds it well under %v", elapsed, limit)
	}
}

// The budget exists for the hostile many-id shape; a push resolving ONE id —
// the legitimate case, an app racing the kubelet posting its own id — must be
// granted the full configured wait, unclamped.
func TestSingleIDPushKeepsItsFullWait(t *testing.T) {
	const wait = 5 * time.Millisecond
	meta := &blockingMeta{fakeMeta: newMeta()}
	e := NewEnricher(Config{Meta: meta, Wait: wait})

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	e.EnrichLogs(context.Background(), ld)

	if len(meta.waits) != 1 || meta.waits[0] != wait {
		t.Errorf("granted waits = %v, want exactly [%v]", meta.waits, wait)
	}
	if v, ok := rl.Resource().Attributes().Get("k8s.pod.name"); !ok || v.Str() != "web-1" {
		t.Errorf("k8s.pod.name = %q (present=%v), want web-1", v.Str(), ok)
	}
}

// The budget is charged by time actually ELAPSED, never by waits requested: a
// lookup that returns immediately — the wait releases the moment the id
// appears — leaves the budget intact for the ids that genuinely park. A
// requested-wait charge would zero every id past the fourth here.
func TestFastLookupsSpendNoWaitBudget(t *testing.T) {
	const wait = 250 * time.Millisecond
	const ids = 20
	meta := &recordingMeta{fakeMeta: newMeta()} // records waits, returns instantly
	e := NewEnricher(Config{Meta: meta, Wait: wait})

	e.EnrichLogs(context.Background(), ghostLogs(ids))

	if got := len(meta.waits); got != ids {
		t.Fatalf("distinct ids looked up = %d, want %d", got, ids)
	}
	for id, waits := range meta.waits {
		for _, w := range waits {
			if w != wait {
				t.Fatalf("lookup for %s granted %v, want the full %v: fast lookups must not spend the budget", id, w, wait)
			}
		}
	}
}
