// Package store maintains an in-memory view of pod and container metadata,
// indexed by container runtime ID and by node name.
//
// The store is populated from a pod informer (initial LIST, then WATCH
// events). Lookups for container IDs that are not yet known can block until
// the metadata arrives. Metadata for deleted pods and for replaced container
// IDs (container restarts) is retained for a configurable TTL so that
// short-lived workloads can still be resolved shortly after they are gone.
package store

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

// Store is safe for concurrent use.
type Store struct {
	ttl time.Duration
	now func() time.Time

	mu          sync.RWMutex
	pods        map[types.UID]*record
	byContainer map[string]*containerEntry
	byNode      map[string]map[types.UID]*record
	// byPodName indexes pods by "namespace/name". A deleted pod stays
	// resolvable until its tombstone expires or a new pod with the same name
	// replaces it.
	byPodName map[string]*record
	// byPodIP indexes LIVE pods by pod IP, for the agent's opt-in peer-IP
	// resource attribution. hostNetwork pods (PodIP == HostIP, an ambiguous
	// shared address) are excluded, and deleted pods are removed immediately —
	// pod IPs are recycled quickly, so a tombstone must never resolve by IP.
	byPodIP map[string]*record
	// waiters holds blocked GetContainer calls keyed by the normalized
	// container ID they are waiting for; each channel is closed when that
	// specific ID becomes resolvable.
	waiters map[string][]chan struct{}
}

type record struct {
	pod             kubemeta.Pod
	ownerRefs       []metav1.OwnerReference
	resourceVersion string
	// containerIDs are the normalized IDs currently reported by the pod.
	containerIDs map[string]struct{}
	// expireAt is zero while the pod exists in the cluster; once the pod is
	// deleted it holds the tombstone expiry time.
	expireAt time.Time
}

type containerEntry struct {
	podUID    types.UID
	container kubemeta.Container
	// expireAt is zero while the ID is currently reported by a live pod.
	expireAt time.Time
}

// New creates a store that retains metadata for deleted pods and replaced
// container IDs for ttl. A ttl <= 0 disables the tombstone cache.
func New(ttl time.Duration) *Store {
	return &Store{
		ttl:         ttl,
		now:         time.Now,
		pods:        make(map[types.UID]*record),
		byContainer: make(map[string]*containerEntry),
		byNode:      make(map[string]map[types.UID]*record),
		byPodName:   make(map[string]*record),
		byPodIP:     make(map[string]*record),
		waiters:     make(map[string][]chan struct{}),
	}
}

// ContainerResult is the outcome of a successful container lookup. Pod.Owners
// is left nil; the caller resolves the chain from OwnerRefs.
type ContainerResult struct {
	Container kubemeta.Container
	Pod       kubemeta.Pod
	OwnerRefs []metav1.OwnerReference
}

// NodePod is one pod scheduled on a node.
type NodePod struct {
	Pod       kubemeta.Pod
	OwnerRefs []metav1.OwnerReference
}

// UpsertPod records the current state of a pod. It is called for informer
// add and update events (including the initial list).
func (s *Store) UpsertPod(p *corev1.Pod) {
	pod, containers := kubemeta.FromPod(p)

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := s.pods[p.UID]
	if rec != nil && rec.expireAt.IsZero() && rec.resourceVersion == p.ResourceVersion {
		return // periodic resync, nothing changed
	}
	var oldNode, oldIP string
	var oldIDs map[string]struct{}
	if rec == nil {
		rec = &record{}
		s.pods[p.UID] = rec
	} else {
		oldNode = rec.pod.NodeName
		oldIP = rec.pod.PodIP
		oldIDs = rec.containerIDs
	}

	rec.pod = pod
	rec.ownerRefs = append([]metav1.OwnerReference(nil), p.OwnerReferences...)
	rec.resourceVersion = p.ResourceVersion
	rec.expireAt = time.Time{} // resurrect if a late update follows a delete

	ids := make(map[string]struct{}, len(containers))
	for id, c := range containers {
		ids[id] = struct{}{}
		s.byContainer[id] = &containerEntry{podUID: p.UID, container: c}
		// Wake exactly the requests blocked on this container ID.
		if ws := s.waiters[id]; len(ws) > 0 {
			for _, ch := range ws {
				close(ch)
			}
			delete(s.waiters, id)
		}
	}
	rec.containerIDs = ids

	// IDs this pod reported before but no longer does have aged out of the
	// kubelet's status (e.g. a second restart); keep them resolvable for ttl.
	for id := range oldIDs {
		if _, ok := ids[id]; ok {
			continue
		}
		if e := s.byContainer[id]; e != nil && e.podUID == p.UID && e.expireAt.IsZero() {
			s.expireEntryLocked(id, e)
		}
	}

	if oldNode != pod.NodeName {
		s.removeFromNodeLocked(oldNode, p.UID)
	}
	if pod.NodeName != "" {
		m := s.byNode[pod.NodeName]
		if m == nil {
			m = make(map[types.UID]*record)
			s.byNode[pod.NodeName] = m
		}
		m[p.UID] = rec
	}
	s.byPodName[pod.Namespace+"/"+pod.Name] = rec

	ip := pod.PodIP
	if ip == pod.HostIP {
		ip = "" // hostNetwork: the node IP is shared, not an identity
	}
	if finishedPhase(pod.Phase) {
		// A finished pod's status may retain a podIP the CNI has already
		// recycled; it must never claim (or keep) the IP mapping.
		ip = ""
	}
	if oldIP != ip && oldIP != "" && s.byPodIP[oldIP] == rec {
		delete(s.byPodIP, oldIP)
	}
	if ip != "" {
		s.byPodIP[ip] = rec
	}
}

// DeletePod tombstones a pod. Its metadata (and its container IDs) remain
// resolvable for the configured TTL; the pod stops being reported as a
// scrape target immediately.
func (s *Store) DeletePod(uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec := s.pods[uid]
	if rec == nil {
		return
	}
	now := s.now()
	s.removeFromNodeLocked(rec.pod.NodeName, uid)
	if rec.pod.PodIP != "" && s.byPodIP[rec.pod.PodIP] == rec {
		delete(s.byPodIP, rec.pod.PodIP)
	}

	if s.ttl <= 0 {
		for id := range rec.containerIDs {
			if e := s.byContainer[id]; e != nil && e.podUID == uid {
				delete(s.byContainer, id)
			}
		}
		s.dropNameIndexLocked(rec)
		delete(s.pods, uid)
		return
	}

	deletedAt := now
	rec.pod.DeletedAt = &deletedAt
	rec.expireAt = now.Add(s.ttl)
	for id := range rec.containerIDs {
		if e := s.byContainer[id]; e != nil && e.podUID == uid && e.expireAt.IsZero() {
			e.expireAt = rec.expireAt
		}
	}
}

// GetContainer looks up metadata by container ID (with or without the
// runtime scheme prefix). If the ID is not yet known it blocks until the
// metadata for that specific container arrives or ctx is done — waiting is
// per container ID, not global on the cache. The initial lookup always
// happens, so an already-expired ctx degrades to a non-blocking lookup.
func (s *Store) GetContainer(ctx context.Context, id string) (ContainerResult, bool) {
	id = kubemeta.NormalizeContainerID(id)
	if id == "" {
		return ContainerResult{}, false
	}
	// Fast path: read lock only.
	s.mu.RLock()
	res, ok, gone := s.lookupLocked(id)
	s.mu.RUnlock()
	if ok {
		return res, true
	}
	if gone {
		// Expired tombstone: the container is definitively deleted, so
		// waiting for its metadata to (re)appear would just burn the budget.
		return ContainerResult{}, false
	}
	for {
		// Double-checked: the ID may have been indexed since the read-locked
		// miss (e.g. every waiter waking at once); re-checking under the read
		// lock keeps such lookup bursts from serializing on the write lock.
		s.mu.RLock()
		res, ok, gone = s.lookupLocked(id)
		s.mu.RUnlock()
		if ok || gone {
			return res, ok
		}
		s.mu.Lock()
		res, ok, gone = s.lookupLocked(id)
		if ok || gone {
			s.mu.Unlock()
			return res, ok
		}
		ch := make(chan struct{})
		s.waiters[id] = append(s.waiters[id], ch)
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			s.removeWaiter(id, ch)
			return ContainerResult{}, false
		case <-ch:
			// The ID was indexed; loop to fetch it.
		}
	}
}

func (s *Store) removeWaiter(id string, ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ws := s.waiters[id]
	for i, c := range ws {
		if c == ch {
			s.waiters[id] = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(s.waiters[id]) == 0 {
		delete(s.waiters, id)
	}
}

// lookupLocked resolves a normalized container ID. gone reports an expired
// (present-but-unswept) tombstone: the ID was known and its pod is
// definitively deleted, so callers must not block waiting for it.
func (s *Store) lookupLocked(id string) (res ContainerResult, ok, gone bool) {
	e := s.byContainer[id]
	if e == nil {
		return ContainerResult{}, false, false
	}
	now := s.now()
	if !e.expireAt.IsZero() && now.After(e.expireAt) {
		return ContainerResult{}, false, true
	}
	rec := s.pods[e.podUID]
	if rec == nil || (!rec.expireAt.IsZero() && now.After(rec.expireAt)) {
		return ContainerResult{}, false, true
	}
	return ContainerResult{Container: e.container, Pod: rec.pod, OwnerRefs: rec.ownerRefs}, true, false
}

// GetPodByName returns the pod with the given namespace and name; deleted
// pods stay resolvable (with DeletedAt set) until their tombstone expires or
// a new pod with the same name replaces them.
func (s *Store) GetPodByName(namespace, name string) (NodePod, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec := s.byPodName[namespace+"/"+name]
	if rec == nil || (!rec.expireAt.IsZero() && s.now().After(rec.expireAt)) {
		return NodePod{}, false
	}
	return NodePod{Pod: rec.pod, OwnerRefs: rec.ownerRefs}, true
}

// GetPodByUID returns the pod with the given UID. Deleted pods stay
// resolvable until their tombstone expires (as with the container endpoint),
// so pushed telemetry that lags a pod deletion still attributes correctly.
func (s *Store) GetPodByUID(uid string) (NodePod, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec := s.pods[types.UID(uid)]
	if rec == nil || (!rec.expireAt.IsZero() && s.now().After(rec.expireAt)) {
		return NodePod{}, false
	}
	return NodePod{Pod: rec.pod, OwnerRefs: rec.ownerRefs}, true
}

// GetPodByIP returns the live pod owning the given pod IP, if any. Deleted
// and finished pods never resolve (their IP may already belong to a new
// pod), and hostNetwork pods are not indexed.
func (s *Store) GetPodByIP(ip string) (NodePod, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec := s.byPodIP[ip]
	if rec == nil || rec.pod.DeletedAt != nil || finishedPhase(rec.pod.Phase) {
		return NodePod{}, false
	}
	return NodePod{Pod: rec.pod, OwnerRefs: rec.ownerRefs}, true
}

// finishedPhase reports whether a pod phase means the pod has stopped
// running (its IP is eligible for reuse by the CNI).
func finishedPhase(phase string) bool {
	return phase == "Succeeded" || phase == "Failed"
}

// PodsOnNode returns all live pods scheduled on the given node.
func (s *Store) PodsOnNode(node string) []NodePod {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m := s.byNode[node]
	out := make([]NodePod, 0, len(m))
	for _, rec := range m {
		out = append(out, NodePod{Pod: rec.pod, OwnerRefs: rec.ownerRefs})
	}
	return out
}

// Stats reports current cache sizes.
func (s *Store) Stats() (pods, containers int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pods), len(s.byContainer)
}

// Sweep removes expired tombstones. It is exported for tests; Run calls it
// periodically.
func (s *Store) Sweep() {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, e := range s.byContainer {
		if !e.expireAt.IsZero() && now.After(e.expireAt) {
			delete(s.byContainer, id)
		}
	}
	removed := false
	for uid, rec := range s.pods {
		if !rec.expireAt.IsZero() && now.After(rec.expireAt) {
			s.dropNameIndexLocked(rec)
			delete(s.pods, uid)
			removed = true
		}
	}
	if removed {
		// Container entries always expire no later than their pod's record,
		// so this pass normally removes nothing; it is defensive.
		for id, e := range s.byContainer {
			if s.pods[e.podUID] == nil {
				delete(s.byContainer, id)
			}
		}
	}
}

// Run sweeps expired tombstones until ctx is done.
func (s *Store) Run(ctx context.Context) {
	interval := s.ttl / 4
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Sweep()
		}
	}
}

func (s *Store) expireEntryLocked(id string, e *containerEntry) {
	if s.ttl <= 0 {
		delete(s.byContainer, id)
		return
	}
	e.expireAt = s.now().Add(s.ttl)
}

// dropNameIndexLocked removes rec from the name index unless a newer pod
// with the same name has already replaced it.
func (s *Store) dropNameIndexLocked(rec *record) {
	key := rec.pod.Namespace + "/" + rec.pod.Name
	if s.byPodName[key] == rec {
		delete(s.byPodName, key)
	}
}

func (s *Store) removeFromNodeLocked(node string, uid types.UID) {
	if node == "" {
		return
	}
	m := s.byNode[node]
	if m == nil {
		return
	}
	delete(m, uid)
	if len(m) == 0 {
		delete(s.byNode, node)
	}
}
