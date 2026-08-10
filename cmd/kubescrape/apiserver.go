package main

// The API-server reachability watchdog: the ACTIVE signal that the served
// cache may have stopped advancing.
//
// MAY, and the hedge is the honest word. What is measured is narrow: whether a
// NEW connection from this pod reaches the API server. That is a proxy for "the
// caches can refill", not a reading of them — a blackholed ESTABLISHED watch
// leaves the cache frozen and still probes healthy, and one lost packet probes
// sick without anything having been missed (apiserverProbe carries the detail).
//
// It exists because no passive signal reports the commonest outage RELIABLY.
// kubescrape_informer_watch_errors_total fires on what the API server ANSWERS
// (a 403 on the LIST, a deleted CRD, a rejected watch) and on a failed RELIST —
// client-go's ListAndWatchWithContext returns the list error and
// RunWithContext hands any returned error to the watch-error handler. But while
// the server is merely UNREACHABLE the watch REQUEST keeps failing retriably
// (connection refused), and the reflector backs off and retries the watch
// inside watchWithResync without ever returning, so it never relists and
// nothing is reported: measured silent across four outage shapes of up to five
// minutes, with one real outage where the counter stepped five seconds AFTER
// recovery. /readyz latches at the initial sync by design; and the store gauges
// do not even freeze — the tombstone sweeper keeps running over a store nothing
// refills, so kubescrape_store_pods DECAYS. A measured 60s blackout produced:
// /readyz 200 throughout, no watch-error series, and fourteen log lines, every
// one INFO.

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/metadata"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/owners"
)

const (
	// apiserverWarnEvery re-states a PERSISTING outage at this cadence. The
	// transition warns immediately; after that the condition is unchanged and
	// one line per probe would bury the recovery line in it.
	apiserverWarnEvery = time.Minute

	// apiserverProbeTimeout bounds ONE probe, capped further by the interval.
	// A refused connection fails in milliseconds, but a blackholed network
	// (the shape a NetworkPolicy or a dead node produces) hangs until the
	// deadline — and a probe outliving its own interval would keep reporting
	// the previous window while the current one goes unmeasured.
	apiserverProbeTimeout = 10 * time.Second
)

// apiserverWatchdog probes the API server on an interval and publishes the
// verdict. The zero value is not usable; call newAPIServerWatchdog.
type apiserverWatchdog struct {
	probe    func(context.Context) error
	interval time.Duration
	timeout  time.Duration
	log      *slog.Logger

	// reachable starts TRUE: nothing has failed yet, and the first probe runs
	// the moment Run starts. Claiming an outage before measuring one would
	// page an operator every time the process boots.
	reachable atomic.Bool
	failures  atomic.Int64
	// warn throttles the persisting-outage line (logdedupe.Throttle is the
	// repo's primitive for exactly this).
	warn logdedupe.Throttle
	// downSince stamps the transition, and failuresAtDown snapshots the
	// lifetime counter there so both log lines can report failures for THIS
	// outage — the number beside a per-outage duration has to have the same
	// scope, or an operator reads a process-lifetime total as the count for the
	// minute that just ended. The lifetime total is the metric
	// (kubescrape_apiserver_probe_failures_total), not the log. Only the probe
	// goroutine touches either field.
	downSince      time.Time
	failuresAtDown int64
	now            func() time.Time
}

// newAPIServerWatchdog builds a watchdog around probe. It registers nothing
// and starts nothing — startAPIServerWatchdog does both.
func newAPIServerWatchdog(probe func(context.Context) error, interval time.Duration, log *slog.Logger) *apiserverWatchdog {
	w := &apiserverWatchdog{
		probe:    probe,
		interval: interval,
		timeout:  min(apiserverProbeTimeout, interval),
		log:      log,
		now:      time.Now,
	}
	w.reachable.Store(true)
	return w
}

// startAPIServerWatchdog registers the two metrics and starts the probe loop,
// or does NEITHER when the interval is not positive.
//
// The registration is inside the guard deliberately: with the probe off, an
// absent kubescrape_apiserver_reachable says "nobody is looking", which an
// alert can select for, while a published 0 would say "unreachable" and a
// published 1 would be a claim the process never checked. Returns nil when
// disabled, so a caller cannot accidentally drive a watchdog nothing publishes.
func startAPIServerWatchdog(ctx context.Context, probe func(context.Context) error, interval time.Duration, log *slog.Logger) *apiserverWatchdog {
	if interval <= 0 {
		return nil
	}
	w := newAPIServerWatchdog(probe, interval, log)
	obs.RegisterAPIServerProbe(w.reachable.Load, w.failures.Load)
	go w.run(ctx)
	return w
}

// run probes immediately and then every interval until ctx is done.
func (w *apiserverWatchdog) run(ctx context.Context) {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		w.probeOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// probeOnce runs one probe and updates the gauge, the counter and the log.
func (w *apiserverWatchdog) probeOnce(ctx context.Context) {
	pctx, cancel := context.WithTimeout(ctx, w.timeout)
	err := w.probe(pctx)
	cancel()

	if err != nil && ctx.Err() != nil {
		// Our own shutdown cancelled the request. Reporting it as an outage
		// would stamp a 0 on the gauge and a WARN in the log on every clean
		// termination — the failure would be the process, not the cluster.
		return
	}
	if err != nil {
		failures := w.failures.Add(1)
		transition := w.reachable.Swap(false)
		if transition {
			w.downSince = w.now()
			// This probe is the first of the outage, so the snapshot excludes
			// it: failures - failuresAtDown == 1 on the transition line.
			w.failuresAtDown = failures - 1
		}
		// The transition always speaks; a persisting outage speaks on the
		// throttle. Flapping therefore costs at most one WARN plus one INFO
		// per interval, which is itself the news.
		//
		// Allow is called BEFORE the disjunction, never inside it: `transition
		// || Allow(...)` short-circuits, so the transition's own line would not
		// CLAIM the throttle slot and the very next probe would re-warn — the
		// per-probe flood the throttle exists to prevent.
		allow := w.warn.Allow(apiserverWarnEvery)
		if transition || allow {
			// Says what was MEASURED — a new connection did not reach the API
			// server — and what that does and does not imply. The probe cannot
			// see the watch streams: the informers may still be receiving
			// events over connections established before the outage, so "the
			// cache stopped advancing" would be a claim, not an observation.
			w.log.Warn("API server unreachable from this pod; the metadata cache may have stopped advancing, so responses may be stale",
				"error", err, "failedProbes", failures-w.failuresAtDown,
				"unreachableFor", w.now().Sub(w.downSince).Round(time.Second),
				"note", "/readyz stays 200 by design — an unready service loses its endpoints and would cut every agent off a cache that is still useful")
		}
		return
	}
	if !w.reachable.Swap(true) {
		// failedProbes is this outage's, matching unreachableFor beside it; the
		// process-lifetime total is kubescrape_apiserver_probe_failures_total.
		w.log.Info("API server reachable again from this pod",
			"unreachableFor", w.now().Sub(w.downSince).Round(time.Second),
			"failedProbes", w.failures.Load()-w.failuresAtDown)
	}
}

// apiserverProbe is the probe main runs: a metadata-only LIST of ONE namespace.
//
// Chosen over an AbsPath("/readyz") probe for two reasons. RBAC: /readyz is a
// non-resource URL, and while the default system:public-info-viewer binding
// usually grants it, "usually" is not a guarantee the shipped ClusterRole
// makes — a 403 there would report a permanent outage that is not one, which is
// worse than no signal. Fidelity: /readyz reports the API SERVER's opinion of
// itself, while this exercises the same TCP → TLS → authn → authz → etcd path
// the informers' relists take, from this pod, with this ServiceAccount. That is
// what "the cache can refill" actually depends on.
//
// What it cannot do is observe the informers' OWN connections. This request
// gets its own (a pooled one while that is alive, a fresh dial once it is not),
// so what it reports is whether this pod can reach the API server NOW — not
// whether the watch streams are still delivering. A watch blackholed mid-flight
// (cache frozen, socket still ESTABLISHED) probes healthy, and one dropped
// packet probes unhealthy without the caches having missed anything. It is a
// reachability signal, not a liveness reading of the caches.
//
// Namespaces because the service already lists and watches them (the
// owner-chain metadata informers, hence owners.NamespaceGVR — reusing the
// constant is what keeps "needs no RBAC we do not already hold" structural);
// they are cluster-scoped, few, and Limit=1 with the METADATA client makes the
// response one PartialObjectMetadata whatever the cluster's size. No
// ResourceVersion is set on purpose: RV="0" is served from the API server's
// watch cache and would still succeed while etcd is unavailable, which is
// precisely a state in which the informers cannot relist.
func apiserverProbe(mc metadata.Interface) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := mc.Resource(owners.NamespaceGVR).List(ctx, metav1.ListOptions{Limit: 1})
		return err
	}
}
