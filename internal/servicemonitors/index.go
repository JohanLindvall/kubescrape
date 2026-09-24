package servicemonitors

// The Index: the monitor store the informer feeds, its change token, and the
// news protocol that tells a real update from a resync re-delivery.

import (
	"cmp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Index is the thread-safe monitor store fed by the informer.
type Index struct {
	mu          sync.RWMutex
	monitors    map[string]*Monitor
	podMonitors map[string]*PodMonitor
	// The resourceVersion of the object last REJECTED under each key, per kind,
	// so upsertMonitor can tell a newly broken monitor from the same broken
	// monitor re-delivered by a resync. It holds nothing the index serves — a
	// rejected monitor is exactly the one that is not indexed — and it is
	// bounded by the broken monitors that exist: an entry is dropped when the
	// key parses again (upsertMonitor) and when the object goes away
	// (deleteMonitor, which clears it BEFORE its own not-indexed early return,
	// since a rejected key is never in the monitor map).
	//
	// Per kind and not one shared map: the key is "namespace/name", which a
	// ServiceMonitor and a PodMonitor may both carry.
	rejectedMonitors    map[string]string
	rejectedPodMonitors map[string]string
	// gen changes on every mutation, so a consumer that derives something
	// expensive from the whole index (the server's monitor→services cross
	// product) can hold it until the index actually changes instead of until a
	// timer lapses. Atomic and read without the lock: a stale read only costs
	// one extra rebuild.
	gen atomic.Uint64

	// The AuthSecretRefs memo, on that same token. Its own mutex, never mu: the
	// build takes mu for reading, and a read path that took the index's WRITE
	// lock would serialise every /v1/scrape-auth request against the informer.
	// authBuilds counts full harvests, for tests.
	authMu     sync.Mutex
	authRefs   AuthRefs
	authGen    uint64
	authValid  bool
	authBuilds atomic.Int64
}

// Generation changes whenever the indexed monitors change. It is a change
// TOKEN, not a count: compare it with a previously observed value, never
// interpret the difference.
func (ix *Index) Generation() uint64 { return ix.gen.Load() }

// NewIndex creates an empty index.
func NewIndex() *Index {
	return &Index{
		monitors:            make(map[string]*Monitor),
		podMonitors:         make(map[string]*PodMonitor),
		rejectedMonitors:    make(map[string]string),
		rejectedPodMonitors: make(map[string]string),
	}
}

// upsertMonitor is the ONE invalid-update-removes policy behind Upsert and
// UpsertPodMonitor: a monitor UPDATED to an unparseable spec is removed rather
// than kept, because silently serving the previous version forever would
// diverge from what the manifest declares (prometheus-operator likewise
// generates no config for an invalid monitor) — and the stale endpoints carry
// the secret refs AuthSecretRefs allowlists, so keeping them would leave
// /v1/scrape-auth willing to serve a Secret the live spec no longer names.
//
// The parse already happened, outside the lock; one write-lock hold then
// either stores the monitor or deletes the key. The two arms used different
// lock choreography for the same observable behavior (the ServiceMonitor arm
// re-locked via Delete on the error path); the single-hold form is kept for
// both — each branch is still exactly one atomic map transition, so a
// concurrent reader sees the same states as before.
//
// It is also where the CHANGE TOKEN is moved, and only a real change may move
// it. An informer resync re-delivers every monitor byte-identical, and the
// token is what holds the server's monitor→Service cross product together
// (buildMonitoredServices: 19.8 ms and 9.67 MB at 50 monitors x 2,000
// Services) — bumping it for a re-delivery meant a `-resync`-configured
// service rebuilt that cross product on essentially every agent poll. Three
// deliveries leave the TOKEN alone: the same resourceVersion stored under the
// same key, a failed parse for a key that is already absent, and (in Delete) a
// key that was not there. An EMPTY resourceVersion counts as changed — only
// hand-built objects have one, and for those the version says nothing about the
// content.
//
// It REPORTS that decision as well as acting on it. The change token settles
// what the index does; the CALLER has reporting of its own that is just as
// event-shaped — the metadata service logs a monitor's uninterpreted fields and
// counts kubescrape_monitor_fields_ignored_total, and it logs and counts a
// monitor it could not parse — and a re-delivery re-fired all of it. With
// `-resync 10m`, fifty monitors carrying a `relabelings` or a `sampleLimit`
// (ordinary in a prometheus-operator install) meant fifty WARN lines every ten
// minutes forever and a counter whose RATE tracked the resync period rather
// than anything an operator changed, which is not a thing an alert can be
// written against. The sibling refusal in the same handler chain was demoted to
// Debug for exactly this reason, and said so.
//
// So the reported bool is "this delivery is NEWS", which is what an event
// report needs, and the ERROR path is included in that — it is the branch the
// gate is easiest to get wrong. `!had` cannot stand in for "already reported":
// a monitor that never parsed is not indexed either, so the FIRST sighting of a
// broken monitor and the thousandth resync of it are the same map lookup, and
// gating on the index alone would silence exactly the report an operator needs
// (an applied monitor doing nothing) while still firing forever for the one it
// does not. The rejected-version tables are what separate them: news is a
// monitor DROPPED from the index, a key never rejected before, or a rejection
// at a resourceVersion different from the one last rejected — with an empty
// resourceVersion news every time, the same rule the success path applies and
// for the same reason.
func upsertMonitor[M any, P interface {
	*M
	version() string
}](ix *Index, monitors map[string]P, rejected map[string]string, key, resourceVersion string, m P, err error) (news bool, _ error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if err != nil {
		_, had := monitors[key]
		if had {
			// The invalid-update-removes policy: this one really did change
			// what is served, so the token moves and the report is news
			// whatever was rejected here before.
			delete(monitors, key)
			ix.gen.Add(1)
		}
		previous, seen := rejected[key]
		rejected[key] = resourceVersion
		return had || !seen || resourceVersion == "" || previous != resourceVersion, err
	}
	// It parses now, so a later failure is news again.
	delete(rejected, key)
	if cur, ok := monitors[key]; ok && m.version() != "" && cur.version() == m.version() {
		return false, nil // re-delivery of the object already indexed
	}
	monitors[key] = m
	ix.gen.Add(1)
	return true, nil
}

// Upsert parses and stores a ServiceMonitor (see upsertMonitor for the
// invalid-update-removes policy).
func (ix *Index) Upsert(u *unstructured.Unstructured) error {
	_, _, err := ix.UpsertChanged(u)
	return err
}

// UpsertChanged is Upsert, additionally reporting whether the delivery was
// NEWS — false for the byte-identical re-delivery an informer resync makes of
// every object it holds, including a re-delivery of an object that does not
// parse. A caller whose logging or metrics describe an EVENT rather than a
// state gates them on it; see upsertMonitor for what the distinction costs when
// it is not made, and for why the error path cannot be gated on the index alone.
//
// eps is the parsed monitor's endpoints (nil on error), handed back so the
// caller's per-change report reads the very object this call parsed instead of
// looking it up again under a second lock.
func (ix *Index) UpsertChanged(u *unstructured.Unstructured) (eps []Endpoint, news bool, err error) {
	m, err := Parse(u)
	news, err = upsertMonitor(ix, ix.monitors, ix.rejectedMonitors,
		u.GetNamespace()+"/"+u.GetName(), u.GetResourceVersion(), m, err)
	if err != nil {
		return nil, news, err
	}
	return m.Endpoints, news, nil
}

// Delete removes a monitor.
func (ix *Index) Delete(namespace, name string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	deleteMonitor(ix, ix.monitors, ix.rejectedMonitors, namespace+"/"+name)
}

// deleteMonitor removes a key and moves the change token only if it was there.
// A delete for a key the index never held (a monitor that failed to parse, a
// DeletedFinalStateUnknown replay) changes nothing, and the token's whole job
// is to say when something changed. Caller holds the write lock.
//
// The rejection record goes FIRST and unconditionally, before that early
// return: a rejected key is precisely the one that is not in the monitor map,
// so clearing it afterwards would never run — leaving the entry until the
// process exits, and making a re-created monitor that is broken the same way
// report nothing.
func deleteMonitor[M any](ix *Index, monitors map[string]*M, rejected map[string]string, key string) {
	delete(rejected, key)
	if _, had := monitors[key]; !had {
		return
	}
	delete(monitors, key)
	ix.gen.Add(1)
}

// keyedMonitor is what sortedMonitors needs from a kind: both have it through
// the embedded monitorBase.
type keyedMonitor interface {
	key() (namespace, name string)
}

// sortedMonitors collects a monitor map's values ordered by (namespace, name):
// map iteration order must not decide which monitor a URL-deduped target is
// attributed to. The caller holds at least the read lock. It sorts on the two
// FIELDS (monitorBase.key), never on the "ns/name" map key: '/' (0x2F) sorts
// after '-' (0x2D), so the composite string would order "a-x/…" before "a/…"
// while the field sort orders them the other way.
func sortedMonitors[P keyedMonitor](monitors map[string]P) []P {
	out := make([]P, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b P) int {
		na, nma := a.key()
		nb, nmb := b.key()
		return cmp.Or(strings.Compare(na, nb), strings.Compare(nma, nmb))
	})
	return out
}

// All returns the current monitors, ordered by namespace/name: map iteration
// order must not decide which monitor a URL-deduped target is attributed to
// (the same determinism the server enforces for services).
func (ix *Index) All() []*Monitor {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return sortedMonitors(ix.monitors)
}

// Rejected counts the monitors whose CURRENT object does not parse, by kind.
//
// This is STATE, not an event: upsertMonitor's error arm records the object
// (keyed by resourceVersion, so a resync of the same broken object is not
// news), a later parse success or a Delete removes it, and the count is what
// remains true right now. kubescrape_monitor_parse_errors_total is the event
// half — it says a breakage HAPPENED and never comes back down — while a
// rejected monitor is one whose targets are gone TODAY: an unparseable update
// deletes the monitor from the index, dropping every target it contributed,
// and until this returns to zero some configuration is contributing nothing.
//
// Counts rather than names, because the consumer is a gauge: the names are on
// the parse-failure warn line, and cloning two maps per export to repeat them
// here would be retained-nothing work on every self-metrics tick.
func (ix *Index) Rejected() (serviceMonitors, podMonitors int) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.rejectedMonitors), len(ix.rejectedPodMonitors)
}

// Endpoints returns a stored ServiceMonitor's endpoints (nil when absent).
func (ix *Index) Endpoints(namespace, name string) []Endpoint {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if m := ix.monitors[namespace+"/"+name]; m != nil {
		return m.Endpoints
	}
	return nil
}
