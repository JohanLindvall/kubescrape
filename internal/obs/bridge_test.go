// Tests for the /metrics bridge: kubescrape_* served exactly when the OTLP
// push is off.
package obs

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
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

// End-to-end shape of a bridged histogram: the exposition must carry
// cumulative le buckets plus _sum/_count, matching what the observations
// actually were. A regrouping slip here would ship a plausible-looking but
// wrong distribution.
func TestBridgeHistogramExposition(t *testing.T) {
	for _, v := range []float64{0.003, 0.02, 2} {
		ScrapeDuration.WithLabelValues("bridge-test").Observe(v)
	}
	body := scrapeBody(t, true)
	var got []string
	for _, ln := range strings.Split(body, "\n") {
		if strings.Contains(ln, `pipeline="bridge-test"`) {
			got = append(got, strings.TrimSpace(ln))
		}
	}
	if len(got) == 0 {
		t.Fatalf("no bridged histogram series:\n%.600s", body)
	}
	joined := strings.Join(got, "\n")
	// Default buckets are prometheus.DefBuckets: 0.005 catches the first
	// observation, 0.025 the first two, +Inf all three.
	for _, want := range []string{
		`kubescrape_scrape_duration_seconds_bucket{pipeline="bridge-test",le="0.005"} 1`,
		`kubescrape_scrape_duration_seconds_bucket{pipeline="bridge-test",le="0.025"} 2`,
		`kubescrape_scrape_duration_seconds_bucket{pipeline="bridge-test",le="+Inf"} 3`,
		`kubescrape_scrape_duration_seconds_sum{pipeline="bridge-test"} 2.023`,
		`kubescrape_scrape_duration_seconds_count{pipeline="bridge-test"} 3`,
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in:\n%s", want, joined)
		}
	}
}
