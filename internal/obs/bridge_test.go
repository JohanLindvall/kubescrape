// Tests for the /metrics bridge: kubescrape_* served exactly when the OTLP
// push is off.
package obs

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

func scrapeBody(t *testing.T, internal bool) string {
	t.Helper()
	srv := httptest.NewServer(RuntimeHandler(internal))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// RuntimeHandler(true) serves the kubescrape_* Registry metrics beside the
// runtime collectors; RuntimeHandler(false) — the push-enabled mode — keeps
// /metrics runtime-only so the same series never ship twice.
func TestRuntimeHandlerInternalToggle(t *testing.T) {
	HTTPRequests.WithLabelValues("/bridge-test", "200").Inc()

	with := scrapeBody(t, true)
	if !strings.Contains(with, "kubescrape_http_requests_total") {
		t.Fatalf("internal=true body lacks kubescrape_* metrics:\n%.400s", with)
	}
	if !strings.Contains(with, "go_goroutines") {
		t.Fatal("internal=true body lacks runtime metrics")
	}
	without := scrapeBody(t, false)
	if strings.Contains(without, "kubescrape_") {
		t.Fatal("internal=false body must stay runtime-only")
	}
}

// bridgeSeq gives each run of TestBridgeHistogramExposition its own series.
// Registry is process-global self-telemetry and NEVER expires or resets by
// design (internal/metrics: the 200-year registryExpiration is how it spells
// "never"), so a fixed label value accumulates across `go test -count=N` — the
// exact bucket counts below read 1/2/3 on the first iteration and 2/4/6 on the
// second. Asserting exact counts against a global accumulator is only
// meaningful on a series nothing has touched, so each run mints one.
var bridgeSeq atomic.Int64

// End-to-end shape of a bridged histogram: the exposition must carry
// cumulative le buckets plus _sum/_count, matching what the observations
// actually were. A regrouping slip here would ship a plausible-looking but
// wrong distribution.
func TestBridgeHistogramExposition(t *testing.T) {
	pipeline := fmt.Sprintf("bridge-test-%d", bridgeSeq.Add(1))
	for _, v := range []float64{0.003, 0.02, 2} {
		ScrapeDuration.WithLabelValues(pipeline).Observe(v)
	}
	body := scrapeBody(t, true)
	var got []string
	for ln := range strings.SplitSeq(body, "\n") {
		if strings.Contains(ln, `pipeline="`+pipeline+`"`) {
			got = append(got, strings.TrimSpace(ln))
		}
	}
	if len(got) == 0 {
		t.Fatalf("no bridged histogram series:\n%.600s", body)
	}
	joined := strings.Join(got, "\n")
	// durationBuckets: 0.005 catches the first observation, 0.025 the first
	// two, +Inf all three.
	for _, want := range []string{
		fmt.Sprintf(`kubescrape_scrape_duration_seconds_bucket{pipeline=%q,le="0.005"} 1`, pipeline),
		fmt.Sprintf(`kubescrape_scrape_duration_seconds_bucket{pipeline=%q,le="0.025"} 2`, pipeline),
		fmt.Sprintf(`kubescrape_scrape_duration_seconds_bucket{pipeline=%q,le="+Inf"} 3`, pipeline),
		fmt.Sprintf(`kubescrape_scrape_duration_seconds_sum{pipeline=%q} 2.023`, pipeline),
		fmt.Sprintf(`kubescrape_scrape_duration_seconds_count{pipeline=%q} 3`, pipeline),
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in:\n%s", want, joined)
		}
	}
}

// The scrape and export durations are bounded by -scrape-timeout and
// -otlp-timeout, 15s by default. On the default buckets, whose top finite bound
// is 10s, every attempt between 10s and the timeout — and every timed-out one —
// landed in +Inf, where histogram_quantile answers with the highest finite
// bound: the documented "p90 approaching -otlp-timeout" alert flatlined at 10
// and could never cross a threshold near 15. A 12s attempt must land in a
// finite bucket at or below the timeout.
func TestDurationHistogramsResolvePastTheDefaultTimeouts(t *testing.T) {
	label := fmt.Sprintf("duration-test-%d", bridgeSeq.Add(1))
	ScrapeDuration.WithLabelValues(label).Observe(12)
	ExportDuration.WithLabelValues(label).Observe(12)
	body := scrapeBody(t, true)
	for _, want := range []string{
		fmt.Sprintf(`kubescrape_scrape_duration_seconds_bucket{pipeline=%q,le="10"} 0`, label),
		fmt.Sprintf(`kubescrape_scrape_duration_seconds_bucket{pipeline=%q,le="15"} 1`, label),
		fmt.Sprintf(`kubescrape_export_duration_seconds_bucket{signal=%q,le="10"} 0`, label),
		fmt.Sprintf(`kubescrape_export_duration_seconds_bucket{signal=%q,le="15"} 1`, label),
		fmt.Sprintf(`kubescrape_export_duration_seconds_bucket{signal=%q,le="60"} 1`, label),
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q: a duration under the default timeout must resolve to a finite bucket", want)
		}
	}
}

// The bridge's refusal arm is the only thing between a point client_golang
// will not build and a series silently ABSENT from /metrics — which with
// -self-metrics-interval=0 is the ONLY delivery path for kubescrape_*. Both
// refusals it can meet are driven here on a private registry: a label NAME
// the exposition reserves, and a label VALUE that is not valid UTF-8 (the
// reachable one — client_golang validates values, internal/metrics does not).
// The good series must still be served, each bad one counted, and one WARN
// must name the metric.
func TestBridgeRefusalIsCountedAndNamed(t *testing.T) {
	r := metrics.NewRegistry()
	r.Counter("kubescrape_bridge_good_total", "good").Inc()
	r.CounterVec("kubescrape_bridge_badvalue_total", "bad value", "gate").WithLabelValues("\xff").Inc()
	r.CounterVec("kubescrape_bridge_badname_total", "bad name", "__reserved").WithLabelValues("x").Inc()

	buf := &lockedBuf{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	defer slog.SetDefault(prev)

	preg := prometheus.NewRegistry()
	preg.MustRegister(registryCollector{reg: r})
	srv := httptest.NewServer(promhttp.HandlerFor(preg, promhttp.HandlerOpts{}))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200: one refused point must not fail the whole scrape\n%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "kubescrape_bridge_good_total 1") {
		t.Errorf("the valid series is missing:\n%s", body)
	}
	for _, bad := range []string{"kubescrape_bridge_badvalue_total", "kubescrape_bridge_badname_total"} {
		if strings.Contains(string(body), bad+"{") {
			t.Errorf("%s was served; client_golang should have refused it:\n%s", bad, body)
		}
	}
	if got := r.SkippedPoints(); got != 2 {
		t.Errorf("SkippedPoints = %d, want 2: a refused point must be counted, not silently absent", got)
	}
	if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "metric=kubescrape_bridge_bad") {
		t.Errorf("want a WARN naming the refused metric, got:\n%s", out)
	}
}

// lockedBuf is a log sink safe to read after a handler goroutine wrote to it:
// the write happens on the server's goroutine and nothing the race detector
// can see orders it before the test's read.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
