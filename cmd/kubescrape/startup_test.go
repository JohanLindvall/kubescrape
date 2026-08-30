package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/logfmt"

	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// summaryLines renders the startup dump through the PRODUCTION handler and maps
// each line's pairs by its msg — the summary's job is to be greppable logfmt,
// so it is checked as logfmt.
func summaryLines(t *testing.T, apiServer string) map[string]map[string]string {
	t.Helper()
	var buf bytes.Buffer
	logStartupSummary(slog.New(cli.NewLogfmtHandler(&buf, slog.LevelInfo)), apiServer)
	out := map[string]map[string]string{}
	for _, line := range bytes.Split(bytes.TrimSuffix(buf.Bytes(), []byte("\n")), []byte("\n")) {
		if err := logfmt.Validate(line); err != nil {
			t.Fatalf("summary line is not logfmt: %v\n%s", err, line)
		}
		pairs := map[string]string{}
		var msg string
		_ = logfmt.Iterate(line, func(k, v []byte) bool {
			if string(k) == "msg" {
				msg = string(v)
			} else {
				pairs[string(k)] = string(v)
			}
			return true
		})
		out[msg] = pairs
	}
	return out
}

// The dump is what an operator has on a first live run. The API server in
// particular is the one destination nobody passes explicitly — it comes from
// the in-cluster environment — so without it the commonest failure ("nothing is
// in the store") cannot be diagnosed from the log at all.
func TestStartupSummaryNamesTheAPIServerListenersAndLimits(t *testing.T) {
	lines := summaryLines(t, "https://10.96.0.1:443")
	want := map[string][]string{
		"effective configuration": {"servicemonitors", "monitorNamespaces", "scrapeAuthSecrets", "selfAttributes", "logLevel"},
		"effective destinations":  {"apiServer", "kubeconfig"},
		"effective listeners":     {"listen", "metricsListen", "pprofListen"},
		"effective identity":      {"namespace", "serviceName", "instance", "selfMetricsInterval"},
		"effective limits":        {"waitTimeout", "maxBlockedLookups", "cacheTTL", "metadataCacheTTL", "resync", "apiserverProbeInterval"},
	}
	for msg, keys := range want {
		pairs, ok := lines[msg]
		if !ok {
			t.Errorf("the summary has no %q line", msg)
			continue
		}
		for _, k := range keys {
			if _, ok := pairs[k]; !ok {
				t.Errorf("%q does not report %q: %v", msg, k, pairs)
			}
		}
	}
	if got := lines["effective destinations"]["apiServer"]; got != "https://10.96.0.1:443" {
		t.Errorf("apiServer = %q, want the resolved host", got)
	}
	if got := lines["effective identity"]["serviceName"]; got != "kubescrape" {
		t.Errorf("serviceName = %q, want the name serviceSelfResource stamps", got)
	}
}

// The token file is a PATH, and -monitor-namespaces renders in a stable order:
// a summary whose value reorders between restarts is a diff nobody can read.
func TestStartupSummaryReportsPathsAndAStableNamespaceList(t *testing.T) {
	tokenFile, nss := *scrapeAuthTokenFile, *monitorNamespaces
	t.Cleanup(func() { *scrapeAuthTokenFile, *monitorNamespaces = tokenFile, nss })
	*scrapeAuthTokenFile = "/var/run/secrets/kubescrape/scrape-auth-token"
	*monitorNamespaces = "b, a ,c"

	cfg := summaryLines(t, "x")["effective configuration"]
	if cfg["scrapeAuthTokenFile"] != "/var/run/secrets/kubescrape/scrape-auth-token" {
		t.Errorf("scrapeAuthTokenFile = %q, want the path", cfg["scrapeAuthTokenFile"])
	}
	if cfg["monitorNamespaces"] != "b,a,c" {
		t.Errorf("monitorNamespaces = %q, want the flag's order preserved", cfg["monitorNamespaces"])
	}
	*monitorNamespaces = ""
	if got := summaryLines(t, "x")["effective configuration"]["monitorNamespaces"]; got != "(all)" {
		t.Errorf("monitorNamespaces = %q with the flag empty, want (all) — an empty value reads as 'none honoured'", got)
	}
}

// syncBuffer is a bytes.Buffer a watchdog goroutine writes while the test reads
// it. The handler writes from another goroutine, so the buffer needs the lock —
// the race detector is right about that, and a sleep-then-read test would only
// hide it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// /readyz LATCHES on the cache sync, so a cache that never syncs is a 503 for
// the process lifetime: no Service endpoints, and every agent in the fleet
// blocked on a lookup. client-go can only report "not synced"; the gate NAME is
// what tells an operator which RBAC rule is missing.
func TestWaitForCachesNamesTheCacheThatWillNotSync(t *testing.T) {
	grace, rewarn := cacheSyncGrace, cacheSyncReWarn
	cacheSyncGrace, cacheSyncReWarn = 10*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { cacheSyncGrace, cacheSyncReWarn = grace, rewarn })

	var pods, replicasets atomic.Bool
	pods.Store(true)
	gates := []syncGate{{"pods", pods.Load}, {"replicasets", replicasets.Load}}
	var buf syncBuffer
	ready := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go waitForCaches(ctx, gates, store.New(time.Minute), slog.New(cli.NewLogfmtHandler(&buf, slog.LevelInfo)), ready)

	deadline := time.After(2 * time.Second)
	for !strings.Contains(buf.String(), "replicasets") {
		select {
		case <-deadline:
			t.Fatalf("no warning naming the unsynced cache:\n%s", buf.String())
		case <-time.After(time.Millisecond):
		}
	}
	if strings.Contains(buf.String(), "caches=pods") {
		t.Errorf("a cache that HAS synced was reported as pending:\n%s", buf.String())
	}
	select {
	case <-ready:
		t.Fatal("readiness was announced with a cache still unsynced")
	default:
	}

	replicasets.Store(true)
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("ready was never closed after every cache synced")
	}
	if !strings.Contains(buf.String(), "informer caches synced") {
		t.Errorf("the sync was not reported:\n%s", buf.String())
	}
}

// The metric half: one gauge per cache, so a replica stuck unready — which by
// construction has no Service endpoints and nobody to curl it — is still
// visible to an alert.
func TestGateStatesReportEveryCache(t *testing.T) {
	var synced atomic.Bool
	states := gateStates([]syncGate{{"pods", synced.Load}, {"services", func() bool { return true }}})
	got := states()
	if len(got) != 2 || got["pods"] || !got["services"] {
		t.Fatalf("states = %v, want pods pending and services synced", got)
	}
	synced.Store(true)
	if got := states(); !got["pods"] {
		t.Errorf("states = %v, want pods synced once its informer is", got)
	}
}

// The informer transform's "cannot happen" branch: an object whose metadata is
// unreadable goes into the cache UNTRIMMED, and the only symptom is RSS
// climbing with managedFields nothing can read. Silent before; throttled now.
func TestUntrimmableInformerObjectIsReportedOnce(t *testing.T) {
	var buf syncBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(cli.NewLogfmtHandler(&buf, slog.LevelInfo)))
	t.Cleanup(func() { slog.SetDefault(old) })
	transformWarn = logdedupe.Throttle{}
	t.Cleanup(func() { transformWarn = logdedupe.Throttle{} })

	for range 3 {
		got, err := stripManagedFields("not an api object")
		if err != nil || got != "not an api object" {
			t.Fatalf("stripManagedFields returned (%v, %v), want the object back unchanged", got, err)
		}
	}
	out := buf.String()
	if n := strings.Count(out, "cached untrimmed"); n != 1 {
		t.Errorf("logged %d lines for 3 failures, want exactly 1 (the throttle):\n%s", n, out)
	}
	if !strings.Contains(out, "type=string") {
		t.Errorf("the line does not name the type it could not read:\n%s", out)
	}
}
