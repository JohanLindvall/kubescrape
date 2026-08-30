// Package services maintains an in-memory index of Services so pods can be
// matched against the Services that select them (for service-annotation
// based scrape discovery).
package services

import (
	"maps"
	"sort"
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Service is the subset of a Kubernetes Service needed for scrape discovery.
// Instances are immutable once published; do not mutate them.
type Service struct {
	Name        string
	Namespace   string
	UID         string
	Labels      map[string]string
	Annotations map[string]string
	Selector    map[string]string
	Ports       []Port
	// resourceVersion is the informer object this record was derived from. It
	// exists only so Upsert can recognise a re-delivery that changes nothing;
	// it is not served and not part of the model.
	resourceVersion string
}

// Port is one service port with its target-port mapping.
type Port struct {
	Name string
	Port int32
	// TargetPortName is set when targetPort references a named container
	// port; otherwise TargetPortNum holds the numeric target port (0 means
	// unset, in which case the target port equals Port).
	TargetPortName string
	TargetPortNum  int32
}

// Index is safe for concurrent use.
type Index struct {
	mu          sync.RWMutex
	byNamespace map[string]map[types.UID]*Service
	// byName maps "namespace/name" to the UID currently holding it. Services
	// are keyed by UID, so a name reused by a RECREATED Service is the one
	// collision the UID map cannot see — see Upsert.
	byName map[string]types.UID
	// gen changes on every mutation, so a consumer that derives something
	// expensive from the whole index (the server's monitor→services cross
	// product) can hold it until the index actually changes instead of until a
	// timer lapses. Atomic and read without the lock: a stale read only costs
	// one extra rebuild.
	gen atomic.Uint64
	// nameReused counts Services that arrived under a namespace/name a
	// DIFFERENT UID still held — the missed-delete guard in Upsert firing.
	nameReused atomic.Int64
	// reads counts locked reads; see Reads.
	reads atomic.Int64

	// sortMu guards the InNamespaces memo below. It is a SEPARATE lock from mu
	// and is never held across it for longer than a map read: the two-phase
	// shape in InNamespaces takes sortMu, releases it, takes mu.RLock to build
	// what was missing, releases that, and takes sortMu again to store. Lock
	// order is therefore sortMu → mu, and nothing else in this package takes
	// sortMu at all.
	sortMu sync.Mutex
	// sorted memoises the per-namespace, name-sorted snapshot InNamespaces
	// returns, valid for sortedGen. A nil entry means "this namespace has no
	// Services", which must be remembered as a fact or an empty namespace
	// rebuilds on every request.
	//
	// It is invalidated WHOLESALE on any change to the index, exactly like the
	// server's monitor→Service cross product: gen is a single token for the
	// whole index, and a Service edit is rare while a targets request is once
	// per node per scrape cycle. Upsert already ignores a re-delivery whose
	// resourceVersion is unchanged, so an informer resync does not bump gen and
	// does not empty this.
	sorted    map[string][]*Service
	sortedGen uint64
}

// Generation changes whenever the indexed services change. It is a change
// TOKEN, not a count: compare it with a previously observed value, never
// interpret the difference.
func (ix *Index) Generation() uint64 { return ix.gen.Load() }

// NameReuses counts Services that arrived under a namespace/name a different
// UID still held (see the nameReused field). Published through
// obs.RegisterStoreAnomalies.
func (ix *Index) NameReuses() int64 { return ix.nameReused.Load() }

// Reads counts the times the index has been read under its lock.
//
// It is here because the READ PATTERN is the property worth pinning, and
// nothing else can see it: matching a node's pods used to take the RLock and
// scan the whole namespace once PER POD, so a 110-pod node in a 1,000-Service
// namespace did 110 lock round trips and 110,000 selector evaluations per
// targets request — on the default annotation path, while the two sibling
// lookups on the same request had both already been hoisted out of that loop.
func (ix *Index) Reads() int64 { return ix.reads.Load() }

// NewIndex creates an empty index.
func NewIndex() *Index {
	return &Index{
		byNamespace: make(map[string]map[types.UID]*Service),
		byName:      make(map[string]types.UID),
	}
}

// Upsert records the current state of a service.
func (ix *Index) Upsert(svc *corev1.Service) {
	// CopyMeta filters the annotations, like pods, owners and namespaces: a
	// Service is the fourth annotation-bearing object this API serves and was
	// the one missed. Its annotations ride on every service- and monitor-derived
	// target on the UNAUTHENTICATED /v1/nodes/{node}/targets route, and the
	// Services that get there are exactly the hand-annotated ones most likely to
	// carry a kubectl last-applied-configuration — a verbatim copy of the whole
	// applied object, including anything inlined into it.
	labels, annotations := kubemeta.CopyMeta(svc.Labels, svc.Annotations)
	// The annotation budget's refusal, counted once per informer event with
	// the object's kind (see obs.MetadataAnnotationsOmitted). A Service's
	// annotations ride on every service- and monitor-derived target, so they
	// are already inside scrape.MaxTargetBytesPerPod — this is the door that
	// says one was served short.
	if kubemeta.AnnotationsOmitted(annotations) {
		obs.MetadataAnnotationsOmitted.WithLabelValues("Service").Inc()
	}
	// The selector is a PLAIN copy: it is label-matching input, not served
	// metadata, so the annotation filter must never apply to it. nil-for-empty
	// like the meta maps.
	var selector map[string]string
	if len(svc.Spec.Selector) > 0 {
		selector = maps.Clone(svc.Spec.Selector)
	}
	rec := &Service{
		Name:            svc.Name,
		Namespace:       svc.Namespace,
		UID:             string(svc.UID),
		Labels:          labels,
		Annotations:     annotations,
		Selector:        selector,
		resourceVersion: svc.ResourceVersion,
	}
	for _, p := range svc.Spec.Ports {
		port := Port{Name: p.Name, Port: p.Port}
		switch p.TargetPort.Type {
		case intstr.String:
			port.TargetPortName = p.TargetPort.StrVal
		case intstr.Int:
			port.TargetPortNum = p.TargetPort.IntVal
		}
		rec.Ports = append(rec.Ports, port)
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()
	m := ix.byNamespace[svc.Namespace]
	// A re-delivery that changes NOTHING must not move the change token. The
	// token is what holds the server's monitor→Service cross product together
	// (buildMonitoredServices: 19.8 ms and 9.67 MB at 50 monitors x 2,000
	// Services), and an informer resync re-delivers every Service byte-identical
	// — so with `-resync` set, an unconditional bump meant essentially every
	// agent poll paid a full rebuild. The pod path (store.UpsertPod) has had
	// this short-circuit all along; the two index paths did not.
	//
	// An EMPTY resourceVersion is treated as changed. Only hand-built objects
	// have one (the informer always sets it), and for those "same version" is
	// not a statement about content — a test or an embedder mutating a fixture
	// in place would otherwise have its update silently ignored.
	if cur := m[svc.UID]; cur != nil && svc.ResourceVersion != "" && cur.resourceVersion == svc.ResourceVersion {
		return
	}
	ix.gen.Add(1)
	if m == nil {
		m = make(map[types.UID]*Service)
		ix.byNamespace[svc.Namespace] = m
	}
	// A Service arriving under a name a DIFFERENT UID still holds means the old
	// one is gone: its name has been reused. Normally its own Delete event
	// handles that, and client-go synthesizes one from a
	// DeletedFinalStateUnknown tombstone. But DeltaFIFO.Replace keys by
	// ns/name and synthesizes Deleted only for keys ABSENT from the relist, so
	// a Service deleted and recreated under the same name INSIDE a relist gap
	// (apiserver restart, etcd compaction, an expired resourceVersion) arrives
	// as an Update carrying a new UID — and nothing ever deletes the old one.
	//
	// Everything here is keyed by UID, so the name index is the only place the
	// collision is visible. Left alone, the stale record keeps matching pods in
	// Matching() and keeps yielding targets derived from a Service
	// configuration that no longer exists — a removed annotation still
	// scraped, or a changed port scraped forever at up=0 — until the process
	// restarts. This is the same guard, for the same reason, that
	// store.UpsertPod applies to pod names.
	nameKey := svc.Namespace + "/" + svc.Name
	if prev, ok := ix.byName[nameKey]; ok && prev != svc.UID {
		// Counted: reaching this branch is EVIDENCE that a Delete was never
		// delivered, which is a statement about this process's watches rather
		// than about the Service. The guard keeps what is SERVED correct, so
		// the counter is the only trace it leaves — a log line here would run
		// under the index write lock on the informer goroutine. Published
		// through obs.RegisterStoreAnomalies beside the pod-name sibling.
		ix.nameReused.Add(1)
		delete(m, prev)
	}
	ix.byName[nameKey] = svc.UID
	m[svc.UID] = rec
}

// Delete removes a service.
func (ix *Index) Delete(namespace string, uid types.UID) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	m := ix.byNamespace[namespace]
	if m == nil {
		return
	}
	svc, ok := m[uid]
	if !ok {
		// A delete for a UID this index never held changes nothing, and the
		// token's whole job is to say when something changed — the rule Upsert
		// states above and servicemonitors' deleteMonitor already implements.
		// The reachable case is the one Upsert's same-name guard handles: a
		// Service recreated inside a relist gap has its predecessor removed
		// there, and the predecessor's own late Delete then arrives for a UID
		// that is gone. Bumping for it would rebuild the whole monitor→Service
		// cross product on the next targets request.
		return
	}
	ix.gen.Add(1)
	// Only if this UID still holds the name: a recreation may already have
	// claimed it above, and a late Delete for the predecessor must not
	// unindex the live successor.
	nameKey := namespace + "/" + svc.Name
	if cur, ok := ix.byName[nameKey]; ok && cur == uid {
		delete(ix.byName, nameKey)
	}
	delete(m, uid)
	if len(m) == 0 {
		delete(ix.byNamespace, namespace)
	}
}

// All returns the services in the given namespaces (nil = every namespace).
// Each Service appears ONCE however often its namespace is named.
func (ix *Index) All(namespaces []string) []*Service {
	ix.reads.Add(1)
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	var out []*Service
	appendNS := func(ns string) {
		for _, svc := range ix.byNamespace[ns] {
			out = append(out, svc)
		}
	}
	if namespaces == nil {
		for ns := range ix.byNamespace {
			appendNS(ns)
		}
		return out
	}
	if len(namespaces) == 1 {
		// The common shape — a monitor selecting its own namespace — has nothing
		// to dedupe and must not pay for the map.
		appendNS(namespaces[0])
		return out
	}
	// The list comes STRAIGHT from a ServiceMonitor's
	// namespaceSelector.matchNames, which prometheus-operator's CRD does not
	// constrain to be unique — so a repeated entry appended a namespace's whole
	// Service set again, multiplying the server's monitor→services memo and the
	// per-request target loop by the repeat count. InNamespaces already guards
	// exactly this (its `out[ns] != nil` check); All was the sibling that
	// forgot. First-occurrence order is preserved: the caller derives the merged
	// relabel chain from the encounter order.
	seen := make(map[string]struct{}, len(namespaces))
	for _, ns := range namespaces {
		if _, dup := seen[ns]; dup {
			continue
		}
		seen[ns] = struct{}{}
		appendNS(ns)
	}
	return out
}

// Matching returns the services in namespace whose selector matches the
// given pod labels. Services without a selector never match.
//
// It takes the lock per call, so a caller matching MANY pods (every pod on a
// node, per targets request) wants InNamespaces + Service.Selects instead: this
// scans every Service in the namespace, and doing that under a fresh RLock once
// per pod put 110 lock round trips and a map walk apiece on the default
// annotation path of every scrape cycle.
func (ix *Index) Matching(namespace string, podLabels map[string]string) []*Service {
	ix.reads.Add(1)
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	var out []*Service
	for _, svc := range ix.byNamespace[namespace] {
		if svc.Selects(podLabels) {
			out = append(out, svc)
		}
	}
	return out
}

// InNamespaces snapshots the services of each named namespace, each list sorted
// by name. Absent namespaces are simply missing from the result; the slices and
// the Services in them are shared and must be treated as immutable (the same
// contract Matching's results carry).
//
// That contract is now LOAD-BEARING rather than merely tidy: since the memo
// below, two concurrent callers receive the SAME backing array, so a caller
// that sorted or appended to a returned list would corrupt every other
// in-flight request instead of only its own. Both callers in this repo copy
// what they keep (server.optInServices builds a fresh slice, matchingServices
// appends into a caller-owned scratch).
//
// The sort is the caller's determinism, taken once here rather than per pod:
// map iteration order must not decide which Service a URL-deduped scrape target
// is attributed to, and a per-pod sort of the same handful of Services is pure
// repetition. Services are keyed by UID, so name ordering is total within a
// namespace only while names are unique — which Kubernetes guarantees, the
// same-name-replacement window in Upsert aside.
//
// The per-namespace lists are MEMOISED against gen, because the repetition does
// not stop at the pod loop: this runs once per GET /v1/nodes/{node}/targets,
// which is once per node per scrape cycle, and it produced a byte-identical
// answer for every one of them until a Service actually changed. Measured over
// a node whose pods span 20 namespaces of 200 Services (interleaved A/B, n=8):
// 64 allocations and 37.6 KiB per call become 4 and 1.46 KiB, and the wall
// clock falls 99.7% (p=0.000) — a gap so far outside this machine's noise floor
// that even a ±80% spread resolves it. The memo is the same
// change-token discipline the server's monitor→Service cross product already
// uses; a churning index simply misses and pays what it always paid.
//
// The gen read happens BEFORE any data is read, and that order is the whole
// correctness argument: an entry may then be stamped with a gen OLDER than the
// data it holds (a writer landing between the two), which costs one extra
// rebuild, while the reverse — stamping stale data with a fresh gen — would
// serve a Service that no longer exists until something else changed.
func (ix *Index) InNamespaces(namespaces []string) map[string][]*Service {
	gen := ix.gen.Load()

	out := make(map[string][]*Service, len(namespaces))
	var missing []string
	usable := true

	ix.sortMu.Lock()
	switch {
	case gen > ix.sortedGen:
		// Everything memoised describes an older index.
		clear(ix.sorted)
		ix.sortedGen = gen
	case gen < ix.sortedGen:
		// Our token read raced a concurrent caller that has already observed a
		// newer one. Build without the memo and store nothing rather than
		// dragging sortedGen backwards, which would throw away valid entries.
		usable = false
	}
	if usable {
		for _, ns := range namespaces {
			list, ok := ix.sorted[ns]
			if !ok {
				missing = append(missing, ns)
				continue
			}
			if list != nil {
				out[ns] = list
			}
		}
	} else {
		missing = namespaces
	}
	ix.sortMu.Unlock()

	if len(missing) == 0 {
		return out
	}

	// Parallel to missing, not a second map: on the miss path — a cold memo, or
	// an index changing between requests — every extra object is paid by the
	// shape that gets no benefit from the memo at all.
	built := make([][]*Service, len(missing))
	ix.reads.Add(1)
	ix.mu.RLock()
	for i, ns := range missing {
		if out[ns] != nil {
			// A caller may name a namespace twice, and both copies reached
			// `missing`. Record the list AGAIN rather than skipping: the store
			// loop below writes built[i] under missing[i], so a hole here would
			// memoise this namespace as EMPTY — a populated namespace that
			// serves no scrape targets until something else changes the index.
			built[i] = out[ns]
			continue
		}
		m := ix.byNamespace[ns]
		if len(m) == 0 {
			continue // a nil entry: remembered below as a fact, not as a miss
		}
		list := make([]*Service, 0, len(m))
		for _, svc := range m {
			list = append(list, svc)
		}
		if len(list) > 1 {
			sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
		}
		built[i] = list
		out[ns] = list
	}
	ix.mu.RUnlock()

	if !usable {
		return out
	}
	ix.sortMu.Lock()
	// Only if the memo still describes the generation we built against; a
	// change that landed meanwhile has already cleared it.
	if ix.sortedGen == gen {
		if ix.sorted == nil {
			ix.sorted = make(map[string][]*Service, len(built))
		}
		for i, ns := range missing {
			ix.sorted[ns] = built[i]
		}
	}
	ix.sortMu.Unlock()
	return out
}

// Selects reports whether this Service's selector matches a pod's labels. A
// Service without a selector never matches (it is externally managed; its
// endpoints are not derived from pods).
func (s *Service) Selects(podLabels map[string]string) bool {
	if len(s.Selector) == 0 {
		return false
	}
	return selects(s.Selector, podLabels)
}

func selects(selector, labels map[string]string) bool {
	for k, v := range selector {
		// Kubernetes selector semantics: the key must be PRESENT and equal.
		// A bare labels[k] != v would let a selector with an empty value
		// match every pod lacking the label entirely.
		got, ok := labels[k]
		if !ok || got != v {
			return false
		}
	}
	return true
}
