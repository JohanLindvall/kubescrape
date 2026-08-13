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
	// reads counts locked reads; see Reads.
	reads atomic.Int64
}

// Generation changes whenever the indexed services change. It is a change
// TOKEN, not a count: compare it with a previously observed value, never
// interpret the difference.
func (ix *Index) Generation() uint64 { return ix.gen.Load() }

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
		delete(m, prev)
	}
	ix.byName[nameKey] = svc.UID
	m[svc.UID] = rec
}

// Delete removes a service.
func (ix *Index) Delete(namespace string, uid types.UID) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.gen.Add(1)
	m := ix.byNamespace[namespace]
	if m == nil {
		return
	}
	if svc, ok := m[uid]; ok {
		// Only if this UID still holds the name: a recreation may already have
		// claimed it above, and a late Delete for the predecessor must not
		// unindex the live successor.
		nameKey := namespace + "/" + svc.Name
		if cur, ok := ix.byName[nameKey]; ok && cur == uid {
			delete(ix.byName, nameKey)
		}
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

// InNamespaces snapshots the services of each named namespace in ONE lock hold,
// each list sorted by name. Absent namespaces are simply missing from the
// result; the slices and the Services in them are shared and must be treated as
// immutable (the same contract Matching's results carry).
//
// The sort is the caller's determinism, taken once here rather than per pod:
// map iteration order must not decide which Service a URL-deduped scrape target
// is attributed to, and a per-pod sort of the same handful of Services is pure
// repetition. Services are keyed by UID, so name ordering is total within a
// namespace only while names are unique — which Kubernetes guarantees, the
// same-name-replacement window in Upsert aside.
func (ix *Index) InNamespaces(namespaces []string) map[string][]*Service {
	ix.reads.Add(1)
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	out := make(map[string][]*Service, len(namespaces))
	for _, ns := range namespaces {
		m := ix.byNamespace[ns]
		if len(m) == 0 || out[ns] != nil {
			continue
		}
		list := make([]*Service, 0, len(m))
		for _, svc := range m {
			list = append(list, svc)
		}
		if len(list) > 1 {
			sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
		}
		out[ns] = list
	}
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
