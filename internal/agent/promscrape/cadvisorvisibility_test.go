package promscrape

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// The cadvisor pipeline's unresolved rows moved NOTHING before this: they
// export, they look healthy, and they have lost their owner chain and their pod
// labels. `ghost` is the body's pod the fake metadata source does not know.
func TestUnresolvedCadvisorRowsAreCountedAndNamed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/metrics/cadvisor") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(cadvisorBody))
	}))
	defer srv.Close()

	var buf strings.Builder
	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, false)
	s.log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s.cfg.Kubelet.NodeMetrics = false

	before := obs.CadvisorUnresolved.WithLabelValues("container").Value()
	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if got := obs.CadvisorUnresolved.WithLabelValues("container").Value(); got <= before {
		t.Errorf("unresolved containers = %v, want more than %v", got, before)
	}
	got := buf.String()
	if !strings.Contains(got, "did not place a cadvisor row") || !strings.Contains(got, "pod=ghost") {
		t.Errorf("the unresolved row was not named at Debug; log %q", got)
	}
	// objectLevel, never "level": slog writes level= itself, and a second pair
	// of that name makes a logfmt reader resolve the record's severity to
	// "container" — the line reads DEBUG to a human and level=container to
	// Loki, where a severity filter drops it. The METRIC label above stays
	// "level"; only the log key moved. internal/cli's
	// TestNoLogCallUsesASlogReservedKey is the repo-wide guard.
	if !strings.Contains(got, "objectLevel=container") {
		t.Errorf("the object level must not collide with slog's level key; log %q", got)
	}
	// The invariant is per LINE: exactly one ` level=` pair (slog's own).
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		if n := strings.Count(line, " level="); n != 1 {
			t.Errorf("line has %d ` level=` pairs, want exactly 1 (slog's own): %q", n, line)
		}
	}
	// The fold decision is the other half of "why does this pod's resource set
	// look like that", and the pause row folds into pod1.
	if !strings.Contains(got, "folded a pod sandbox row") {
		t.Errorf("the sandbox fold was not reported; log %q", got)
	}
}

// A 403 on /metrics/cadvisor takes both kubelet pipelines down on every node at
// once, and "status 403" alone does not say whether the ClusterRole or the
// token is at fault.
func TestKubeletRefusalNamesTheRBACRule(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	var buf strings.Builder
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, &captureExporter{}, false)
	s.log = slog.New(slog.NewTextHandler(&buf, nil))
	if _, err := s.scrapeCadvisor(context.Background()); err == nil {
		t.Fatal("a 403 scrape reported success")
	}
	got := buf.String()
	for _, want := range []string{"kubelet refused the scrape", "pipeline=cadvisor", "status=403", "nodes/metrics"} {
		if !strings.Contains(got, want) {
			t.Errorf("log is missing %q; got %q", want, got)
		}
	}
	// Once per process: the operator has to go and edit a ClusterRole, and
	// until they do there is nothing new to say — but the counter keeps moving.
	before := obs.ScrapeFailures.WithLabelValues(pipelineCadvisor, reasonUnauthorized).Value()
	_, _ = s.scrapeCadvisor(context.Background())
	s.reportScrapeFailure(pipelineCadvisor, srv.URL, srv.URL, &statusError{code: 403}, false)
	if n := strings.Count(got, "kubelet refused the scrape"); n != 1 {
		t.Errorf("logged %d times, want 1", n)
	}
	if now := obs.ScrapeFailures.WithLabelValues(pipelineCadvisor, reasonUnauthorized).Value(); now <= before {
		t.Errorf("the counter stopped moving with the log line (%v -> %v)", before, now)
	}
}
