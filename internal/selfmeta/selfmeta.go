// Package selfmeta resolves the pod a kubescrape process runs in and stamps
// that pod's Kubernetes resource attributes onto the metrics the process
// generates about ITSELF — the agent's self-metrics and span metrics, the
// metadata service's self-metrics. Those resources are built from what a
// process can see unaided (service.name, version, node or hostname), which is
// not enough to correlate them with the pod they came from.
//
// The two binaries differ only in how they find the pod: the agent asks the
// metadata service (`GET /v1/self`, attributed by the connection's source
// address), the service reads its own store. Both then apply the result
// fill-if-absent, so an identity the process already established for itself
// always wins.
package selfmeta

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

const (
	// fastTries bounds the faster retrying below. A process that is not a
	// resolvable pod (hostNetwork, an address-rewriting hop, no Kubernetes at
	// all) never resolves, and must not retry every few seconds for its whole
	// lifetime.
	fastTries = 6
	// lookupTimeout bounds one resolve attempt.
	lookupTimeout = 10 * time.Second
)

// retryEvery is the retry interval for the FIRST lookup: a process and the
// metadata it needs come up together, so it should not spend a whole refresh
// period exporting unattributed metrics. A var so tests need not sleep through
// it (the tailer's retryBackoff pattern).
var retryEvery = 5 * time.Second

// Resolve returns the pod the calling process runs in.
type Resolve func(ctx context.Context) (*kubemeta.Pod, error)

// Start resolves the pod in the background and keeps it fresh, returning a
// provider that yields nil until the first successful lookup.
//
// Nothing waits on it: a process's own metrics must flow even when the lookup
// cannot succeed — they are how that failure is diagnosed — so the pod
// attributes simply appear on the first export after it does.
func Start(ctx context.Context, resolve Resolve, refresh time.Duration, log *slog.Logger) func() *kubemeta.Pod {
	if log == nil {
		log = slog.Default()
	}
	if refresh <= 0 {
		refresh = time.Minute
	}
	var current atomic.Pointer[kubemeta.Pod]
	fetch := func() bool {
		fctx, cancel := context.WithTimeout(ctx, lookupTimeout)
		defer cancel()
		pod, err := resolve(fctx)
		if err != nil || pod == nil {
			log.Debug("resolving this pod's own metadata", "error", err)
			return false
		}
		current.Store(pod)
		return true
	}
	go func() {
		for tries := 0; !fetch(); tries++ {
			delay := retryEvery
			if tries >= fastTries {
				delay = refresh
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
		if pod := current.Load(); pod != nil {
			log.Info("resolved this pod's own metadata", "namespace", pod.Namespace, "pod", pod.Name)
		}
		ticker := time.NewTicker(refresh)
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

// Exporter is the metrics half of an export chain — what the self-metrics and
// span-metrics loops consume.
type Exporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// Build fills res with the attributes derived from pod. Callers supply their
// own: the agent runs its configured `self` attribute pipeline, the metadata
// service the built-in k8s mapping.
type Build func(res pcommon.Resource, pod *kubemeta.Pod)

// exporter stamps the resolved pod's attributes onto every resource of every
// payload it forwards.
//
// Fill-if-absent, the same rule the ingest enricher applies to a sender's
// resource: what the process already set about itself wins. That is what keeps
// the agent's service.instance.id the node (stable across restarts, unlike the
// pod UID it would otherwise be derived from) and its service.name
// "kubescrape-agent" rather than the DaemonSet's name.
//
// It stamps at export time rather than building one resource at startup so the
// attributes appear without a restart once the lookup succeeds — the processes
// that need them and the metadata they need come up together — and so a pod's
// changed labels are picked up. Nothing shared is mutated: the caller renders a
// fresh payload per export.
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
	if pod := e.pod(); pod != nil {
		built := pcommon.NewResource()
		e.build(built, pod)
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			FillAbsent(built.Attributes(), rms.At(i).Resource().Attributes())
		}
	}
	return e.next.ExportMetrics(ctx, md)
}

// FillAbsent adds src's attributes to dst, never overwriting a key dst has.
func FillAbsent(src, dst pcommon.Map) {
	src.Range(func(k string, v pcommon.Value) bool {
		if _, exists := dst.Get(k); !exists {
			v.CopyTo(dst.PutEmpty(k))
		}
		return true
	})
}
