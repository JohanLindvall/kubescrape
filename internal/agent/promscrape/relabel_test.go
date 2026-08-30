package promscrape

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func TestRelabelKeepDrop(t *testing.T) {
	var c relabelCache
	f, _, err := c.session([]kubemeta.RelabelRule{
		{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "go_.*"},
		{Action: "keep", SourceLabels: []string{"job", "instance"}, Regex: "api;.*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	lbls := []Label{{Name: "job", Value: "api"}, {Name: "instance", Value: "i1"}}
	if f.Keep("go_goroutines", lbls) {
		t.Fatal("drop rule ignored")
	}
	if !f.Keep("http_requests_total", lbls) {
		t.Fatal("kept sample dropped")
	}
	if f.Keep("http_requests_total", []Label{{Name: "job", Value: "web"}}) {
		t.Fatal("keep rule ignored (join 'web;' must not match 'api;.*')")
	}
	// Anchoring: partial matches must not count.
	f2, _, _ := c.session([]kubemeta.RelabelRule{{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "go"}})
	if !f2.Keep("go_goroutines", nil) {
		t.Fatal("unanchored partial match dropped a sample")
	}
	// Bad regex fails the session (never silently exports what was dropped).
	if _, _, err := c.session([]kubemeta.RelabelRule{{Action: "drop", Regex: "("}}); err == nil {
		t.Fatal("bad regex must error")
	}
	// nil for rule-less targets.
	if f, _, _ := c.session(nil); f != nil {
		t.Fatal("no rules must yield nil session")
	}
}

// The compiled-chain cache is bounded: a controller minting monitors with
// distinct (templated/hashed) regexes must not leak a compiled chain per
// fingerprint for the process' life. Evict-one at the cap keeps it hot.
func TestRelabelCacheBounded(t *testing.T) {
	var c relabelCache
	for i := 0; i < maxRelabelChains*3; i++ {
		f, _, err := c.session([]kubemeta.RelabelRule{
			{Action: "drop", SourceLabels: []string{"__name__"}, Regex: fmt.Sprintf("metric_%d_.*", i)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if f == nil {
			t.Fatal("nil session for a rule-bearing target")
		}
		// An evicted chain must not invalidate a live session (it holds its own
		// compiled slice): its drop rule still matches its own metric name.
		if f.Keep(fmt.Sprintf("metric_%d_x", i), nil) {
			t.Fatalf("session %d did not apply its own compiled chain (matching name not dropped)", i)
		}
	}
	c.mu.Lock()
	n := len(c.m)
	c.mu.Unlock()
	if n > maxRelabelChains {
		t.Errorf("cache holds %d chains, want <= %d", n, maxRelabelChains)
	}
	if n == 0 {
		t.Error("cache emptied itself: every scrape would recompile its chain")
	}
}

type fakeAuth struct{ calls int }

func (f *fakeAuth) ScrapeAuth(_ context.Context, ref string) (string, error) {
	f.calls++
	return "tok-" + ref, nil
}

// A target carrying AuthSecret scrapes with the resolved bearer token; the
// token is cached across scrapes.
func TestScrapeTargetBearerAuth(t *testing.T) {
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte("m 1\n"))
	}))
	t.Cleanup(srv.Close)
	auth := &fakeAuth{}
	exp := &captureExporter{}
	tgt := testTarget(srv.URL)
	tgt.AuthSecret = "ns/tok/token"
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{tgt}, Auth: auth,
		Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())
	// Every target is scheduled now, so clear the schedule to stand in for an
	// interval elapsing; the point of the test is that two SCRAPES resolve the
	// token once.
	s.setSchedule(nil, nil)
	s.cycle(context.Background())
	if got, _ := gotAuth.Load().(string); got != "Bearer tok-ns/tok/token" {
		t.Fatalf("Authorization = %q", got)
	}
	if auth.calls != 1 {
		t.Fatalf("auth calls = %d, want 1 (cached)", auth.calls)
	}
	if exp.points() != 2 {
		t.Fatalf("points = %d", exp.points())
	}
}

// A target with metricRelabelings drops the matching series end-to-end.
func TestScrapeTargetMetricRelabelings(t *testing.T) {
	srv := serveBody(t, "go_goroutines 5\nhttp_requests_total 7\n")
	exp := &captureExporter{}
	tgt := testTarget(srv.URL)
	tgt.MetricRelabelings = []kubemeta.RelabelRule{
		{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "go_.*"},
	}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:  staticTargets{tgt},
		Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())
	if exp.points() != 1 {
		t.Fatalf("points = %d, want the go_ series dropped", exp.points())
	}
}
