// Package selfmeta answers "which pod am I, and what surrounds me" for a
// kubescrape process, and stamps that pod's Kubernetes resource attributes
// onto the metrics the process generates about ITSELF — the agent's
// self-metrics and span metrics, the metadata service's self-metrics. Those
// resources are built from what a process can see unaided (service.name,
// version, node or hostname), which is not enough to correlate them with the
// pod they came from.
//
// The two binaries differ only in how they find the pod: the agent asks the
// metadata service (`GET /v1/self`, attributed by the connection's source
// address), the service reads its own store. Both then apply the result
// fill-if-absent, so an identity the process already established for itself
// always wins.
//
// Poll is the background-refresh primitive underneath, generic because the
// agent resolves its node's metadata the same way.
package selfmeta

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// DefaultRefresh is how often the pod a process runs in is re-read, so an
// edited pod label or namespace label reaches the metrics it stamps. The
// re-read is cheap by construction: /v1/self answers with `private, max-age`
// + ETag, so metaclient (a per-process cache — the only kind allowed to hold a
// response that names its caller) serves a fresh entry locally and revalidates
// a stale one as a conditional GET, i.e. a 304 whenever nothing changed.
const DefaultRefresh = time.Minute

// lookupTimeout bounds one resolve attempt.
const lookupTimeout = 10 * time.Second

// firstRetry is the initial retry interval before anything has resolved: a
// process and the metadata it needs come up together, so it must not spend a
// whole refresh period exporting unattributed metrics. It doubles up to the
// refresh interval, so a lookup that will NEVER succeed (hostNetwork, an
// address-rewriting hop, no Kubernetes) settles at the slow cadence instead of
// polling forever. A var so tests need not sleep through it.
var firstRetry = 5 * time.Second

// PollConfig configures Poll.
type PollConfig[T any] struct {
	// Refresh is how often a resolved value is re-read; zero or less disables
	// the lookup entirely (the provider serves Initial forever).
	Refresh time.Duration
	// Initial is served until the first successful resolve. May be nil.
	Initial *T
	// OnFirst, when non-nil, runs once with the first resolved value.
	OnFirst func(*T)
	Log     *slog.Logger
}

// Poll resolves a value in the background, returning a provider that yields
// cfg.Initial (which may be nil) until the first success and the latest
// resolved value after that. cfg.OnFirst is called once with the first
// resolved value — passed in rather than read back through the provider, which
// the caller has not necessarily finished assigning.
//
// Retries before the first success start at firstRetry and double up to
// cfg.Refresh. A failed refresh KEEPS the last good value: a blip must not
// strip attributes off a process's metrics, and stale identity beats none.
//
// Nothing waits on it. A process's own telemetry must flow even when the
// lookup cannot succeed — it is how that failure is diagnosed — so resolved
// values simply appear on the first export after they arrive.
func Poll[T any](ctx context.Context, resolve func(context.Context) (*T, error), cfg PollConfig[T]) func() *T {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	var current atomic.Pointer[T]
	var failures atomic.Int64
	if cfg.Initial != nil {
		current.Store(cfg.Initial)
	}
	if cfg.Refresh <= 0 {
		return current.Load
	}
	fetch := func() *T {
		fctx, cancel := context.WithTimeout(ctx, lookupTimeout)
		defer cancel()
		v, err := resolve(fctx)
		if err != nil || v == nil {
			// First failure at Warn, the rest at Debug: a lookup that never
			// succeeds was previously silent at the default log level, which
			// left "my metrics carry no pod" with no trace anywhere. Repeats
			// stay quiet so a long outage cannot flood the log.
			if failures.Add(1) == 1 {
				log.Warn("resolving own metadata failed; retrying", "error", err)
			} else {
				log.Debug("resolving own metadata", "error", err)
			}
			return nil
		}
		current.Store(v)
		// Reset, so the FIRST failure of each NEW outage is a Warn again.
		// Latched, this counter made every outage after the first Debug-only:
		// a process that resolved once at startup and then could not
		// re-resolve — a recycled pod IP, a store that dropped the record, an
		// RBAC change — went on reporting resolved=1 and stamping stale pod
		// labels with nothing above Debug to say so.
		failures.Store(0)
		return v
	}
	go func() {
		var v *T
		for backoff := firstRetry; ; backoff = min(backoff*2, cfg.Refresh) {
			if v = fetch(); v != nil {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		if cfg.OnFirst != nil {
			cfg.OnFirst(v)
		}
		ticker := time.NewTicker(cfg.Refresh)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fetch()
			}
		}
	}()
	return current.Load
}

// StartPod is Poll for the pod a process runs in, re-read on the given cadence
// so an edited pod or namespace label reaches the metrics it stamps. The
// re-read costs a conditional GET (see DefaultRefresh), and a mis-resolution —
// realistically a pod IP recycled from a predecessor the store has not dropped
// yet — heals on the next one rather than lasting until a restart.
//
// It logs whether the answer is CONFIRMED to be this process's pod (its name
// is the container's hostname). An unconfirmed one is used all the same — a
// custom spec.hostname is legitimate — but it is the thing to look at if the
// attributes ever look like someone else's.
func StartPod(ctx context.Context, resolve func(context.Context) (*kubemeta.Pod, error), refresh time.Duration, log *slog.Logger) func() *kubemeta.Pod {
	if log == nil {
		log = slog.Default()
	}
	return Poll(ctx, resolve, PollConfig[kubemeta.Pod]{
		Refresh: refresh,
		Log:     log,
		OnFirst: func(p *kubemeta.Pod) {
			if !verified(p) {
				// Not provably ours: legitimate under a custom spec.hostname,
				// but also what a proxy or NAT hop in front of the metadata
				// service looks like — and that one silently attributes this
				// process to somebody else's pod.
				log.Warn("resolved a pod that is not provably this one; check for a proxy or NAT hop to the metadata service",
					"namespace", p.Namespace, "pod", p.Name, "hostname", hostname())
				return
			}
			log.Info("resolved this pod's own metadata", "namespace", p.Namespace, "pod", p.Name, "confirmed", true)
		},
	})
}

// verified reports whether pod is provably the pod THIS process runs in: its
// name is this container's hostname, which Kubernetes sets from the pod name
// unless a spec.hostname overrides it. Reported at resolution time, so an
// answer that is not ours is visible rather than merely suspected.
func verified(pod *kubemeta.Pod) bool {
	h := hostname()
	return h != "" && pod.Name == h
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}

// Namespace resolves the pod's own namespace: $POD_NAMESPACE (downward API)
// first, then the ServiceAccount projection every pod with a ServiceAccount
// mounts. Empty means neither was available.
//
// It lives here rather than in the package that first needed it (leader
// election) so that reading it costs no dependency on that package: the
// metadata service wants only this, not an election.
func Namespace() string {
	if ns := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); ns != "" {
		return ns
	}
	b, err := os.ReadFile(namespaceProjection)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// namespaceProjection is the ServiceAccount namespace file. A var so a test
// can point it at a path that definitely does not exist: the "neither source
// available" branch is otherwise unreachable — and silently skipped — whenever
// the suite itself runs inside a pod.
var namespaceProjection = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// SetNamespaceProjectionForTest points Namespace at a different projection path
// and returns the previous one. Tests in OTHER packages need it for the same
// reason this package does: inside a pod the real projection exists, so the
// "no namespace available" branch would silently not run.
func SetNamespaceProjectionForTest(path string) string {
	prev := namespaceProjection
	namespaceProjection = path
	return prev
}

// Exporter is the metrics half of an export chain — what the self-metrics and
// span-metrics loops consume.
type Exporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// Build fills res with the attributes derived from pod, which is NIL until the
// lookup succeeds (and forever where it cannot): a Build must still emit what
// it knows without one — the agent's configured static and Node-template
// attributes — since those are what an alert on an unattributed process
// selects by. Callers supply their own: the agent runs its configured `self`
// attribute pipeline, the metadata service the built-in k8s mapping.
type Build func(res pcommon.Resource, pod *kubemeta.Pod)

// exporter stamps the resolved pod's attributes onto every resource of every
// payload it forwards.
//
// Fill-if-absent (attrs.FillAbsent, the rule the ingest enricher also applies
// to a sender's resource): what the process already set about itself wins.
// That is what keeps the agent's service.instance.id the node (stable across
// restarts, unlike the pod UID it would otherwise be derived from) and its
// service.name "kubescrape-agent" rather than the DaemonSet's name. The
// consequence is that a `self` pipeline can only ADD attributes — it cannot
// override the identity, by design.
//
// It stamps at export time rather than building one resource at startup so the
// attributes appear without a restart once the lookup succeeds — a process and
// the metadata it needs come up together — and so changed pod labels are
// picked up. Nothing shared is mutated: the caller renders a fresh payload per
// export.
type exporter struct {
	next  Exporter
	pod   func() *kubemeta.Pod
	build Build
}

// Wrap returns next with the pod attributes stamped on. A nil provider (the
// feature is off) returns next untouched, so a disabled lookup costs nothing.
func Wrap(next Exporter, pod func() *kubemeta.Pod, build Build) Exporter {
	if pod == nil || build == nil {
		return next
	}
	return &exporter{next: next, pod: pod, build: build}
}

func (e *exporter) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Built even when the pod is UNRESOLVED (a nil Pod is a valid build
	// context): the operator's static attributes — a cluster name, typically —
	// are the labels an alert on kubescrape_self_metadata_resolved == 0 selects
	// by, and gating them on a successful lookup would strip them from exactly
	// the payload that reports the lookup failing.
	built := pcommon.NewResource()
	e.build(built, e.pod())
	if built.Attributes().Len() == 0 {
		return e.next.ExportMetrics(ctx, md)
	}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		attrs.FillAbsent(built.Attributes(), rms.At(i).Resource().Attributes())
	}
	return e.next.ExportMetrics(ctx, md)
}
