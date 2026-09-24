package tailer

// Metadata resolution: attributing a file before any of its data is read
// (containerd lookups under a jittered exponential backoff, plain sources from
// their static attributes), and the one resource renderer, which re-runs when
// the node metadata changes.

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Metadata resolution backoff bounds: the first retry is quick (a container
// genuinely racing the API server resolves within a second or two), the cap
// keeps a permanently unresolvable file down to one blocking call a minute.
const (
	minMetaBackoff = 2 * time.Second
	maxMetaBackoff = time.Minute
)

func nextMetaBackoff(cur time.Duration) time.Duration {
	if cur <= 0 {
		return minMetaBackoff
	}
	if cur >= maxMetaBackoff {
		return maxMetaBackoff
	}
	return min(cur*2, maxMetaBackoff)
}

// jitterMetaBackoff spreads the retry over [d, 1.25d).
//
// Every file on a node is resolved by one goroutine against one service, so a
// metadata-service rollout puts every file on the SAME schedule: they all
// fail together, double together and hit the recovered service together — a
// synchronised burst from every node in the fleet at once, which is how a
// rollout turns into a second outage. The skew is small (the cap stays ~1m)
// but it is enough to decorrelate.
func jitterMetaBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + time.Duration(rand.Int64N(int64(d/4)+1))
}

// resolveMetadata builds the file's resource attributes. Plain files resolve
// from the source's static attributes plus node metadata (immediately, except
// while a CONFIGURED node-info provider has not yielded its first value — see
// resolvePlain); containerd files fetch pod metadata from the service (backing
// off between attempts), and are not consumed until it is available — the data
// waits on disk, nothing is lost.
func (t *Tailer) resolveMetadata(ctx context.Context, f *file) bool {
	if !f.source.containerd {
		return t.resolvePlain(f)
	}
	if time.Now().Before(f.nextMetaTry) {
		return false
	}
	md, err := t.cfg.Metadata.Container(ctx, f.containerID, t.cfg.MetadataWait)
	if err != nil {
		// EXPONENTIAL backoff. The lookup deliberately BLOCKS server-side for
		// the whole -metadata-wait when the container is unknown, and it runs
		// on the single sweep goroutine that serves every file on the node, so
		// a flat retry shorter than the cost of the sweep it spaces out lets a
		// few permanently unresolvable files (a deleted pod whose tombstone has
		// expired) monopolise the tailer: nothing is read, no rotation is
		// noticed, and a file rotating twice inside that window loses the
		// middle incarnation.
		f.metaBackoff = nextMetaBackoff(f.metaBackoff)
		f.nextMetaTry = time.Now().Add(jitterMetaBackoff(f.metaBackoff))
		if metaclient.IsNotFound(err) {
			t.log.Debug("container metadata not found yet", "path", f.path, "id", f.containerID)
		} else if t.metaWarn.Allow(metaWarnEvery) {
			// THROTTLED, and keyless on purpose. This fails per FILE, on each
			// file's own backoff, so an unreachable metadata service made every
			// tracked file on the node warn about the same thing once a minute
			// — a flood proportional to fleet size for one cluster-wide
			// condition. The condition is the service; one example file plus
			// how many are waiting is what an operator acts on, and the rate
			// lives on kubescrape_metadata_requests_total, which metaclient
			// moves per attempt. The waiting count is computed INSIDE the
			// allowed branch: slog evaluates its arguments eagerly, and this
			// walks every tracked file.
			t.log.Warn("fetching container metadata failed; these files are tracked but nothing is read from them until it resolves",
				"path", f.path, "id", f.containerID, "error", err,
				"files", t.unresolvedFiles(), "backoff", f.metaBackoff)
		}
		return false
	}
	f.metaBackoff = 0
	// Retained so the resource can be re-rendered without a second lookup when
	// the NODE metadata changes (refreshNodeAttrs). It is the value metaclient
	// already holds in its per-URL cache — the maps are shared under that
	// cache's treat-as-immutable contract — so this costs one struct per
	// tracked file, not a copy of the pod.
	f.meta = md
	t.buildResource(f, t.nodeInfo())
	f.resolved = true
	if !f.source.wantLabels(md.Pod.Labels) {
		// The source selects pods by label; this one does not match. Labels are
		// only known now, so the file is tracked — but no data is ever read
		// from it, unlike a logs.rules drop which pays read+parse+enrich first.
		f.excluded = true
		t.log.Debug("pod does not match the source selector; not collecting",
			"path", f.path, "source", f.source.name)
		return true
	}
	t.applyPodConfig(f, md.Pod.Annotations)
	return true
}

// unresolvedFiles counts tracked files that have not been attributed yet — the
// scale of an attribution outage, which one file's error cannot convey. Walked
// only from a throttled log branch and from publishStatus, never per file and
// never per line.
func (t *Tailer) unresolvedFiles() int {
	n := 0
	for _, f := range t.files {
		if !f.resolved && !f.excluded {
			n++
		}
	}
	return n
}

// resolvePlain builds a non-containerd file's resource: node attributes from
// the builder plus the source's configured static attributes (which win). A
// source without an explicit service.name defaults it to the source name.
func (t *Tailer) resolvePlain(f *file) bool {
	ni := t.nodeInfo()
	if t.cfg.NodeInfo != nil && ni == nil {
		// A provider is CONFIGURED but has not produced anything yet. Defer:
		// the file stays unresolved (nothing is read before it can be
		// attributed — the containerd path's rule) and the next sweep retries
		// at one cheap provider call per sweep. This is a first-value guard
		// only; it is NOT what keeps the resource current, because it cannot
		// be — the shipped agent seeds selfmeta.Poll with a placeholder
		// NodeInfo (the bare node name, no labels), so the provider is non-nil
		// from the first call and this branch never fires there. What covers
		// the placeholder — and every later node relabel — is refreshNodeAttrs,
		// which re-renders the resource whenever the provider yields a
		// different value.
		return false
	}
	t.buildResource(f, ni)
	f.resolved = true
	return true
}

// nodeInfo reads the configured node-metadata provider once, or nil when none
// is configured. The POINTER it returns is what a file records as "the node
// metadata my resource was built from": selfmeta.Poll stores a freshly
// allocated value on every successful resolve, so pointer inequality is exactly
// "the provider has produced something new since we built".
func (t *Tailer) nodeInfo() *attrs.NodeInfo {
	if t.cfg.NodeInfo == nil {
		return nil
	}
	return t.cfg.NodeInfo()
}

// buildResource renders f.resource from what is known about the file — the
// retained container metadata for a containerd file, the source's statics and
// path captures for a plain one — plus the node metadata passed in, and records
// which node metadata it used.
//
// It is called at resolve time and again whenever the node metadata changes
// (refreshNodeAttrs), so everything that shapes the resource has to live HERE
// rather than at the resolve call site: an override applied once, next to the
// resolve, would be silently dropped by the next re-render. That is why the pod
// annotation's overrides are re-applied from the vetted copy on the file rather
// than re-parsed (re-parsing would also re-count obs.LogPodAttrsRefused and
// re-warn, once per refresh, forever).
func (t *Tailer) buildResource(f *file, ni *attrs.NodeInfo) {
	res := pcommon.NewResource()
	actx := attrs.Context{Node: ni}
	if f.source.containerd {
		if f.meta == nil {
			return // nothing to build from; resolveMetadata has not run
		}
		actx.Pod, actx.Container = &f.meta.Pod, &f.meta.Container
		t.cfg.Attrs.Build(res, actx)
	} else {
		t.cfg.Attrs.Build(res, actx)
		a := res.Attributes()
		if _, ok := f.source.attributes["service.name"]; !ok && f.source.name != "" {
			if _, set := a.Get("service.name"); !set {
				a.PutStr("service.name", f.source.name)
			}
		}
		for k, v := range f.source.attributes {
			a.PutStr(k, v)
		}
		// Path-derived attributes land after the statics: a per-file capture is
		// more specific than the source-wide constant it may share a key with.
		for i := range f.source.pathAttrs {
			f.source.pathAttrs[i].apply(f.path, a)
		}
		// The source's attributes land AFTER Build so they beat templates and
		// defaults — but the pipeline's guarantees must still close over the
		// FINAL set. Re-derive identity (fill-if-absent: a source-declared
		// k8s.namespace.name now yields service.namespace, so tenancy routing
		// and the Mimir job agree about the namespace) and re-apply the
		// operator's global attribute filter (resourceAttributes.disable could
		// not drop a plain source's static attribute while the stamp bypassed
		// it).
		attrs.Identity(res)
		t.cfg.Attrs.FilterResource(res)
	}
	f.resource = res
	f.nodeInfo = ni
	f.applyPodResource(t.cfg.Attrs)
}

// refreshNodeAttrs re-renders a resolved file's resource when the node-metadata
// provider has yielded a different value than the one it was built from.
//
// The resource used to be LATCHED at first resolve, and that was wrong twice
// over. The shipped agent seeds selfmeta.Poll with a placeholder NodeInfo
// carrying the node NAME and no labels, so a file resolved before the first
// GET /v1/nodes/{name}/metadata landed — deterministically every plain-source
// file, which resolves on sweep #1 and takes no lookup at all — kept a
// label-less resource for the whole life of that file while a file discovered a
// second later carried the real one: ONE node exporting two resource shapes at
// once, so an operator template over .Node.Labels (k8s.node.zone, the
// documented example) is missing from half the streams and `sum by
// (k8s.node.zone)` silently drops them. And with no race at all,
// -node-metadata-refresh — which exists to propagate a node relabel, and
// reaches every other pipeline — never reached an already-tailed file.
//
// Re-rendering rather than deferring is the shape every other log producer in
// this repo already has (journald, events, azurediag and ingest rebuild their
// resource per batch, and so self-heal). Deferring instead would gate log
// collection on a lookup that can fail forever: a plain source must not stop
// collecting because the metadata service is unreachable, which is exactly when
// its logs matter most.
//
// It costs one pointer compare per resolved file per sweep, and a re-render
// only when the provider genuinely produced something new — at most once per
// -node-metadata-refresh, and never on the per-line path. Records already built
// are unaffected: flush COPIES f.resource into each ResourceLogs (newScope) and
// the log-metrics bind cache lives for exactly one flush.
func (t *Tailer) refreshNodeAttrs(f *file) {
	if t.cfg.NodeInfo == nil || f.excluded {
		return
	}
	ni := t.cfg.NodeInfo()
	// nil is a provider that has nothing to say (it cannot happen with
	// selfmeta.Poll, whose value only ever moves forward). Keep what we have
	// rather than re-rendering the node attributes away — the same rule as the
	// resolve-time guard, one direction later.
	if ni == nil || ni == f.nodeInfo {
		return
	}
	t.buildResource(f, ni)
}
