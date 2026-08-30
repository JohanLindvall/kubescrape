package promscrape

// Tests for the per-scrape metadata allowance (metabudget.go).
//
// The defect these pin was MEASURED on a live cluster: with -metadata-endpoint
// pointing at a blackholed address, the summary and cadvisor scrapes fetched
// their payloads, parsed every statistic and then threw all of it away, because
// the metadata lookups had consumed the very context the export runs in —
// `up=False dur=15.007s samples=204` for 23 consecutive cycles, while the
// /metrics pipeline beside them (which resolves nothing) kept succeeding.
//
// A blackhole, not a refusal, is the shape that has to be exercised: a refused
// connection returns instantly and was always benign. hangingMetaSource is
// therefore a lookup that returns only when its CONTEXT gives out, which is
// exactly what a dropping firewall produces.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// hangingMetaSource is a metadata service that accepts the connection and never
// answers. Every lookup parks until its context gives out, and the time parked
// is recorded: that total is what the allowance bounds, and measuring it is how
// these tests tell "the lookups were cut short" from "the lookups happened to
// be quick".
type hangingMetaSource struct {
	calls   atomic.Int64
	blocked atomic.Int64
}

func (h *hangingMetaSource) hang(ctx context.Context) error {
	h.calls.Add(1)
	start := time.Now()
	<-ctx.Done()
	h.blocked.Add(int64(time.Since(start)))
	return ctx.Err()
}

func (h *hangingMetaSource) PodByName(ctx context.Context, _, _ string) (*kubemeta.Pod, error) {
	return nil, h.hang(ctx)
}

func (h *hangingMetaSource) Container(ctx context.Context, _ string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	return nil, h.hang(ctx)
}

func (h *hangingMetaSource) parked() time.Duration { return time.Duration(h.blocked.Load()) }

// deadlineExporter is captureExporter plus the one behaviour that decides this
// whole class of bug: an expired context is REFUSED. Both real transports do
// that — the live reproduction's summary failure was literally
// `rpc error: code = DeadlineExceeded` out of grpc-go — and an exporter that
// ignores its context would record a payload production never sent.
type deadlineExporter struct {
	mu      sync.Mutex
	batches []pmetric.Metrics
}

func (d *deadlineExporter) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.batches = append(d.batches, md)
	return nil
}

func (d *deadlineExporter) points() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, b := range d.batches {
		n += b.DataPointCount()
	}
	return n
}

func (d *deadlineExporter) snapshot() []pmetric.Metrics {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]pmetric.Metrics(nil), d.batches...)
}

// hungScrapeBudget is short enough to keep these tests quick and long enough
// that the allowance (half of it) cannot be mistaken for scheduling noise.
const hungScrapeBudget = 600 * time.Millisecond

// A hung metadata service must cost the summary scrape its ATTRIBUTION and
// nothing else. Before the allowance existed, the first lookup parked on the
// scrape's own deadline, so by the time the walk finished there was no context
// left to export in: every statistic the kubelet reported was parsed and then
// dropped.
func TestSummaryScrapeSurvivesAHungMetadataService(t *testing.T) {
	body := loadSummary(t, "summary-1.30.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	meta := &hangingMetaSource{}
	exp := &deadlineExporter{}
	s := newSummaryScraper(t, srv.URL, meta, exp)
	s.cfg.Timeout, s.cfg.Interval = hungScrapeBudget, time.Hour

	start := time.Now()
	n, err := s.scrapeSummary(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("the scrape failed against a hung metadata service: %v", err)
	}
	if n == 0 || exp.points() == 0 {
		t.Fatalf("the scrape parsed %d statistics and exported %d data points; a metadata outage must cost the join, not the data", n, exp.points())
	}
	// The bound itself, measured where it is spent: the lookups may park for at
	// most half the scrape's budget, which is what leaves the other half for the
	// conversion and the export.
	if allowed := hungScrapeBudget / metaBudgetDivisor; meta.parked() > allowed+allowed/2 {
		t.Errorf("the lookups parked for %v, past the %v allowance carved out of a %v scrape budget", meta.parked(), allowed, hungScrapeBudget)
	}
	if elapsed >= hungScrapeBudget {
		t.Errorf("the scrape took %v, i.e. its whole %v budget", elapsed, hungScrapeBudget)
	}

	// "Unattributed data, not no data": the objects carry the identity the
	// payload itself gave them, which is what the label fallback exists for.
	pts := flattenSummary(exp.snapshot())
	var found bool
	for _, p := range pts {
		if p.res["k8s.pod.name"] == "pod1" && p.res["k8s.namespace.name"] == "ns1" {
			found = true
			// service.name is the fallback attrs.ServiceName ends at, and it is
			// what keeps the series carrying a Prometheus job at all.
			if p.res["service.name"] != "pod1" {
				t.Errorf("an unresolved pod's service.name is %v, want the pod name", p.res["service.name"])
			}
		}
	}
	if !found {
		t.Error("no exported point carries the pod identity the summary payload itself named")
	}
}

// The same for the cadvisor scrape, which resolves through the same seam from a
// cgroup path and lost the same way: `scrape failed pipeline=cadvisor
// error="context deadline exceeded"` beside the summary line, every cycle.
func TestCadvisorScrapeSurvivesAHungMetadataService(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(cadvisorBody))
	}))
	defer srv.Close()

	meta := &hangingMetaSource{}
	exp := &deadlineExporter{}
	s := newKubeletScraper(t, srv.URL, meta, exp, false)
	s.cfg.Timeout, s.cfg.Interval = hungScrapeBudget, time.Hour

	n, err := s.scrapeCadvisor(context.Background())
	if err != nil {
		t.Fatalf("the scrape failed against a hung metadata service: %v", err)
	}
	if n == 0 || exp.points() == 0 {
		t.Fatalf("the scrape parsed %d samples and exported %d data points; a metadata outage must cost the join, not the data", n, exp.points())
	}
	if allowed := hungScrapeBudget / metaBudgetDivisor; meta.parked() > allowed+allowed/2 {
		t.Errorf("the lookups parked for %v, past the %v allowance carved out of a %v scrape budget", meta.parked(), allowed, hungScrapeBudget)
	}
	// The rows keep their label identity: that is the cadvisor path's documented
	// fallback and the reason an unresolved row is still worth exporting.
	byIdent := resourcesByIdentity(mergedMetrics(exp.snapshot()))
	if _, ok := byIdent[uid1+"/"+appCID+"/app"]; !ok {
		t.Errorf("no resource carries the row's own cgroup identity; got %v", identityKeys(byIdent))
	}
}

// mergedMetrics folds every exported chunk into one pmetric.Metrics so the
// resource helpers written for a single batch apply.
func mergedMetrics(batches []pmetric.Metrics) pmetric.Metrics {
	out := pmetric.NewMetrics()
	for _, md := range batches {
		md.ResourceMetrics().MoveAndAppendTo(out.ResourceMetrics())
	}
	return out
}

func identityKeys(m map[string]pmetric.ResourceMetrics) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// A healthy metadata service must be unaffected: the allowance bounds a hang,
// it does not ration ordinary lookups. Without this the previous two tests
// would pass just as well against a scraper that had stopped resolving at all.
func TestScrapeStillResolvesWithTheAllowanceInPlace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(cadvisorBody))
	}))
	defer srv.Close()

	meta := &fakeMetaSource{}
	exp := &deadlineExporter{}
	s := newKubeletScraper(t, srv.URL, meta, exp, false)
	s.cfg.Timeout, s.cfg.Interval = hungScrapeBudget, time.Hour

	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if meta.containerCalls.Load() == 0 && meta.podCalls.Load() == 0 {
		t.Fatal("no metadata lookup was issued at all")
	}
	rm, ok := resourcesByIdentity(mergedMetrics(exp.snapshot()))[uid1+"/"+appCID+"/app"]
	if !ok {
		t.Fatal("the app container's resource is missing")
	}
	// Resolved through the metadata service: an owner-derived service.name is
	// something the label fallback cannot produce (it ends at the pod name).
	if got := attrStr(rm.Resource(), "service.name"); got != "dep1" {
		t.Errorf("service.name = %q, want the owner-derived dep1 — the lookup did not resolve", got)
	}
}

// The allowance is spent by MEASURED ELAPSED TIME, and once it is gone no
// further lookup is issued: another parked round trip can only take time from
// the export, and the object it would attribute is exported either way.
func TestMetaLookupStopsIssuingOnceTheAllowanceIsSpent(t *testing.T) {
	s := New(Config{Node: "node1", Targets: staticTargets{}, Exporter: &captureExporter{}})
	ctx, cancel := s.scrapeContext(context.Background(), 200*time.Millisecond, pipelineSummary)
	defer cancel()

	lctx, spent, may := s.metaLookup(ctx)
	if !may {
		t.Fatal("the first lookup of a scrape was refused")
	}
	dl, ok := lctx.Deadline()
	if !ok {
		t.Fatal("a bounded lookup has no deadline")
	}
	if left := time.Until(dl); left > 100*time.Millisecond {
		t.Errorf("the first lookup may run for %v, past the 100ms allowance of a 200ms scrape", left)
	}
	// Park exactly as a blackholed lookup does: until the derived deadline.
	<-lctx.Done()
	spent()

	if _, _, may := s.metaLookup(ctx); may {
		t.Error("a lookup was issued after the allowance was spent")
	}
	// The scrape's own context is untouched: that is the whole point — what is
	// left of it belongs to the conversion and the export.
	if err := ctx.Err(); err != nil {
		t.Errorf("the scrape context is %v after the allowance was spent", err)
	}
}

// No single lookup may take more than what is LEFT: the allowance is a budget
// for the scrape, not a per-lookup timeout that N lookups can each claim.
func TestMetaLookupClampsToTheRemainingAllowance(t *testing.T) {
	s := New(Config{Node: "node1", Targets: staticTargets{}, Exporter: &captureExporter{}})
	ctx, cancel := s.scrapeContext(context.Background(), time.Second, pipelineSummary)
	defer cancel()

	first, spent, may := s.metaLookup(ctx)
	if !may {
		t.Fatal("the first lookup of a scrape was refused")
	}
	<-time.After(300 * time.Millisecond)
	spent()
	_ = first

	second, spent2, may := s.metaLookup(ctx)
	if !may {
		t.Fatal("the second lookup was refused while the allowance still had time in it")
	}
	defer spent2()
	dl, _ := second.Deadline()
	if left := time.Until(dl); left > 250*time.Millisecond {
		t.Errorf("the second lookup may run for %v; only ~200ms of the 500ms allowance was left", left)
	}
}

// A caller outside a scrape carries no allowance and is bounded only by its own
// context — internal/agent/cgroupstats reaches podMeta/containerMeta through
// the exported FillContainerResource and brings its own deadline and its own
// retry policy.
func TestMetaLookupIsUnboundedWithoutAnAllowance(t *testing.T) {
	s := New(Config{Node: "node1", Targets: staticTargets{}, Exporter: &captureExporter{}})
	ctx := context.Background()
	lctx, spent, may := s.metaLookup(ctx)
	defer spent()
	if !may {
		t.Fatal("a lookup outside a scrape was refused")
	}
	if lctx != ctx {
		t.Error("a lookup outside a scrape was given a derived context")
	}
	if _, ok := lctx.Deadline(); ok {
		t.Error("a lookup outside a scrape acquired a deadline")
	}
}

// A blackholed metadata service must not be negative-cached: nothing was
// answered, and a minute of cached "unknown" would keep every object of the
// node unattributed long after the partition healed.
func TestASpentAllowanceIsNotNegativeCached(t *testing.T) {
	meta := &hangingMetaSource{}
	s := New(Config{
		Node: "node1", Targets: staticTargets{}, Exporter: &captureExporter{},
		Kubelet: KubeletConfig{Meta: meta},
	})
	ctx, cancel := s.scrapeContext(context.Background(), 200*time.Millisecond, pipelineSummary)
	defer cancel()

	if p, answered := s.podMeta(ctx, "ns1", "pod1"); p != nil || answered {
		t.Fatalf("a hung lookup reported (%v, answered=%v), want (nil, false)", p, answered)
	}
	if _, ok := s.cacheGet("n\x00ns1/pod1"); ok {
		t.Error("a hung lookup was cached")
	}
	// The allowance is now spent, so the next call must not even ask.
	before := meta.calls.Load()
	if p, answered := s.podMeta(ctx, "ns2", "pod2"); p != nil || answered {
		t.Errorf("a lookup past the allowance reported (%v, answered=%v), want (nil, false)", p, answered)
	}
	if got := meta.calls.Load(); got != before {
		t.Errorf("%d lookups were issued past the allowance, want none", got-before)
	}
	if _, ok := s.cacheGet("n\x00ns2/pod2"); ok {
		t.Error("an unissued lookup was cached")
	}
}

// The exhausted allowance is not silent: the counters an operator already has
// (kubescrape_summary_unresolved_total, kubescrape_metadata_requests_total) say
// that objects went unplaced, and only this line says why.
func TestASpentAllowanceIsWarnedAbout(t *testing.T) {
	var buf strings.Builder
	s := New(Config{
		Node: "node1", Targets: staticTargets{}, Exporter: &captureExporter{},
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})
	ctx, cancel := s.scrapeContext(context.Background(), 20*time.Millisecond, pipelineSummary)

	_, spent, may := s.metaLookup(ctx)
	if !may {
		t.Fatal("the first lookup of a scrape was refused")
	}
	time.Sleep(15 * time.Millisecond)
	spent()
	for range 3 {
		if _, _, may := s.metaLookup(ctx); may {
			t.Fatal("a lookup was issued after the allowance was spent")
		}
	}
	// The line lands when the SCRAPE ends, which is the only moment the shed
	// count is final — and the count is most of what makes it actionable.
	if strings.Contains(buf.String(), "metadata allowance") {
		t.Errorf("the allowance was warned about mid-scrape, where the shed count is not yet final; log was %q", buf.String())
	}
	cancel()
	got := buf.String()
	for _, want := range []string{"metadata allowance", "pipeline=summary", "unattributed=3"} {
		if !strings.Contains(got, want) {
			t.Errorf("the end-of-scrape warning is missing %q; log was %q", want, got)
		}
	}
}

// A scrape that never exhausted its allowance says nothing: the line is about
// an outage, and one per healthy scrape would be a permanent flood.
func TestAnUnspentAllowanceIsSilent(t *testing.T) {
	var buf strings.Builder
	s := New(Config{
		Node: "node1", Targets: staticTargets{}, Exporter: &captureExporter{},
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})
	ctx, cancel := s.scrapeContext(context.Background(), time.Second, pipelineCadvisor)
	_, spent, may := s.metaLookup(ctx)
	if !may {
		t.Fatal("the first lookup of a scrape was refused")
	}
	spent()
	cancel()
	if buf.Len() != 0 {
		t.Errorf("a scrape within its allowance logged %q", buf.String())
	}
}

// context.WithTimeout takes the earlier deadline, so a scrape already at the end
// of its budget bounds the lookup rather than the allowance extending it past
// the scrape.
func TestMetaLookupNeverOutlivesTheScrape(t *testing.T) {
	s := New(Config{Node: "node1", Targets: staticTargets{}, Exporter: &captureExporter{}})
	ctx, cancel := s.scrapeContext(context.Background(), time.Second, pipelineSummary)
	defer cancel()
	scrapeDeadline, _ := ctx.Deadline()

	lctx, spent, may := s.metaLookup(ctx)
	defer spent()
	if !may {
		t.Fatal("the first lookup of a scrape was refused")
	}
	dl, _ := lctx.Deadline()
	if dl.After(scrapeDeadline) {
		t.Errorf("a lookup may run past the scrape's own deadline (%v > %v)", dl, scrapeDeadline)
	}
	// Cancelling the SCRAPE must cancel the lookup with it: a derived context
	// that outlived its parent would leave a hung lookup running after the
	// cycle it belongs to gave up.
	cancel()
	if !errors.Is(lctx.Err(), context.Canceled) {
		t.Errorf("cancelling the scrape left the lookup context at %v, want context.Canceled", lctx.Err())
	}
}
