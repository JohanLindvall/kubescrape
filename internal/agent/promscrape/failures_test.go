package promscrape

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func debugScraper(cfg Config, buf *strings.Builder) *Scraper {
	cfg.Logger = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return New(cfg)
}

// The classification is by TYPE, never by message text: a library rewording an
// error must not silently re-bucket a fleet's failures.
func TestFailureReasonClassifiesByType(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"forbidden", &statusError{code: 403}, reasonUnauthorized},
		{"unauthorized", &statusError{code: 401}, reasonUnauthorized},
		{"other status", &statusError{code: 500}, reasonStatus},
		{"wrapped status", fmt.Errorf("scraping: %w", &statusError{code: 404}), reasonStatus},
		{"sample limit", ErrTooManySamples, reasonSampleLimit},
		{"deadline", fmt.Errorf("get: %w", context.DeadlineExceeded), reasonTimeout},
		{"canceled", fmt.Errorf("get: %w", context.Canceled), reasonCanceled},
		{"unknown authority", fmt.Errorf("get: %w", x509.UnknownAuthorityError{}), reasonTLS},
		{"record header", fmt.Errorf("get: %w", tls.RecordHeaderError{Msg: "first record does not look like TLS"}), reasonTLS},
		{"dns", fmt.Errorf("get: %w", &net.DNSError{Err: "no such host", IsNotFound: true}), reasonDNS},
		{"dns timeout", fmt.Errorf("get: %w", &net.DNSError{Err: "timeout", IsTimeout: true}), reasonTimeout},
		{"refused", fmt.Errorf("get: %w", &net.OpError{Op: "dial", Err: errors.New("connection refused")}), reasonConnect},
		{"classified", classify(reasonExport, errors.New("collector said no")), reasonExport},
		// The explicit wrapper wins over anything the classifier could infer:
		// an export that failed with a deadline is still an export failure, and
		// pointing an operator at the target would be the wrong diagnosis.
		{"classified beats inference", classify(reasonExport, context.DeadlineExceeded), reasonExport},
		{"unknown", errors.New("something new"), reasonOther},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := failureReason(c.err); got != c.want {
				t.Errorf("failureReason(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

// Wrapping must not change what anything downstream reads: the message is the
// cause's verbatim, and errors.Is still reaches through.
func TestClassifiedErrorIsTransparent(t *testing.T) {
	inner := errors.New("boom")
	err := classify(reasonAuth, inner)
	if err.Error() != "boom" {
		t.Errorf("Error() = %q, want the cause verbatim", err.Error())
	}
	if !errors.Is(err, inner) {
		t.Error("errors.Is does not reach the cause")
	}
}

// A 403 on a target must move the counter under `unauthorized` and put the URL
// — the one thing the counter cannot hold — into the log.
func TestScrapeFailureIsCountedAndNamed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	var buf strings.Builder
	before := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonUnauthorized).Value()
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Exporter: &captureExporter{},
		Targets: staticTargets{testTarget(srv.URL)},
	}, &buf)
	s.cycle(context.Background())

	if got := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonUnauthorized).Value(); got != before+1 {
		t.Errorf("unauthorized failures = %v, want %v", got, before+1)
	}
	got := buf.String()
	for _, want := range []string{"scrape failed", "reason=unauthorized", srv.URL, "note="} {
		if !strings.Contains(got, want) {
			t.Errorf("log is missing %q; got %q", want, got)
		}
	}
}

// The line is throttled per (target, reason) — fifty broken targets on two
// hundred nodes would otherwise be 20k identical lines a minute — while the
// counter keeps moving on every cycle.
func TestRepeatedScrapeFailuresAreThrottledButStillCounted(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{Node: "node1", Exporter: &captureExporter{}, Targets: staticTargets{}}, &buf)
	t1 := testTarget("http://a:1")
	before := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonStatus).Value()
	for range 5 {
		s.reportScrapeFailure(pipelineTargets, t1.URL, warnTarget(t1), &statusError{code: 500}, false)
	}
	if got := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonStatus).Value(); got != before+5 {
		t.Errorf("failures = %v, want %v (the counter is the rate)", got, before+5)
	}
	if n := strings.Count(buf.String(), "scrape failed"); n != 1 {
		t.Errorf("logged %d times, want 1 (the rest are inside the window)", n)
	}
	// A failure that changes SHAPE reports at once rather than hiding behind
	// the previous message's window: the remedy has changed.
	s.reportScrapeFailure(pipelineTargets, t1.URL, warnTarget(t1), &statusError{code: 403}, false)
	if n := strings.Count(buf.String(), "scrape failed"); n != 2 {
		t.Errorf("logged %d times, want 2 (a new reason re-fires)", n)
	}
}

// A rolling update cancels every in-flight scrape. That is not the target's
// fault and must not put one accusation per target into the last seconds of
// every deploy — but the counter still records it.
func TestShutdownCancellationIsCountedButNotLogged(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{Node: "node1", Exporter: &captureExporter{}, Targets: staticTargets{}}, &buf)
	before := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonCanceled).Value()
	s.reportScrapeFailure(pipelineTargets, "http://a:1", "cfg", &statusError{code: 500}, true)
	if got := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonCanceled).Value(); got != before+1 {
		t.Errorf("canceled failures = %v, want %v", got, before+1)
	}
	if strings.Contains(buf.String(), "scrape failed") {
		t.Errorf("a shutdown-cancelled scrape logged: %q", buf.String())
	}
}

// The empty target list is the most common first-run failure and it moves no
// other counter at all: no scrape runs, so no scrape fails.
func TestEmptyTargetListWarnsOnceAndRecovers(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{Node: "node1", Exporter: &captureExporter{}, Targets: staticTargets{}}, &buf)

	s.reportTargetSet(nil)
	if obs.ScrapeTargets.Value() != 0 {
		t.Errorf("gauge = %v, want 0", obs.ScrapeTargets.Value())
	}
	if n := strings.Count(buf.String(), "NO scrape targets"); n != 1 {
		t.Fatalf("warned %d times on the transition, want 1; log %q", n, buf.String())
	}
	s.reportTargetSet(nil)
	if n := strings.Count(buf.String(), "NO scrape targets"); n != 1 {
		t.Errorf("warned %d times, want 1 (the condition is unchanged)", n)
	}
	if !strings.Contains(buf.String(), "note=") {
		t.Error("the warning carries no remediation hint")
	}

	s.reportTargetSet([]kubemeta.ScrapeTarget{testTarget("http://a:1")})
	if !strings.Contains(buf.String(), "scrape targets discovered") {
		t.Errorf("the recovery was not reported; log %q", buf.String())
	}
	if obs.ScrapeTargets.Value() != 1 {
		t.Errorf("gauge = %v, want 1", obs.ScrapeTargets.Value())
	}
	// A SECOND outage says so again: the throttle is armed by the transition,
	// not by the clock, or an incident an hour after the last one is silent.
	s.reportTargetSet(nil)
	if n := strings.Count(buf.String(), "NO scrape targets"); n != 2 {
		t.Errorf("warned %d times, want 2 (a fresh outage re-warns)", n)
	}
}

// A failed FETCH must not be read as "this node has no targets": it has its own
// Error line, and blaming discovery for a metadata-service outage sends an
// operator to the wrong place.
func TestAFailedTargetFetchIsNotAnEmptyTargetSet(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Exporter: &captureExporter{},
		Targets: failingTargets{},
	}, &buf)
	s.cycle(context.Background())
	if strings.Contains(buf.String(), "NO scrape targets") {
		t.Errorf("a failed fetch was reported as an empty target set: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "fetching scrape targets") {
		t.Errorf("the fetch failure was not reported: %q", buf.String())
	}
}

// A target dropped by the transforms file's hook is indistinguishable from one
// discovery never returned — same empty list, same silence — so the hook says
// what it took.
func TestTargetHookDropsAreReported(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Exporter: &captureExporter{},
		Targets:    staticTargets{testTarget("http://a:1"), testTarget("http://b:2")},
		TargetHook: func([]kubemeta.ScrapeTarget) []kubemeta.ScrapeTarget { return nil },
	}, &buf)
	s.cycle(context.Background())
	got := buf.String()
	if !strings.Contains(got, "targets: hook changed the target list") || !strings.Contains(got, "dropped=2") {
		t.Errorf("the hook's drops were not reported; log %q", got)
	}
}

// The by-source census is most of the diagnosis when the list is not what an
// operator expected: three different mechanisms produce targets.
func TestTargetSourcesCensus(t *testing.T) {
	pod := testTarget("http://a:1")
	pod.Source = "pod"
	mon := testTarget("http://b:2")
	mon.Source = "servicemonitor"
	bare := testTarget("http://c:3")
	if got := targetSources([]kubemeta.ScrapeTarget{mon, pod, bare}); got != "pod=1,servicemonitor=1,unknown=1" {
		t.Errorf("targetSources = %q", got)
	}
}
