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
	"errors"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta/kubeconvert"
)

// defaultMaxWaiters bounds the number of concurrently blocked GetContainer
// calls. Each waiter pins a map entry (keyed by a client-chosen string) and a
// parked HTTP handler for up to the wait budget, so without a cap a hostile
// client posting distinct garbage IDs could grow the waiter map without
// bound. The cap is far above what a legitimate agent fleet produces (agents
// wait only for containers starting on their own node).
const defaultMaxWaiters = 16384

// maxWaiterIDLen bounds the container-ID strings held as waiter keys. Real
// runtime IDs are 64 hex characters; anything wildly longer is garbage that
// can never appear in a pod status, so blocking (and pinning the bytes as a
// map key) would only serve memory amplification. Such lookups degrade to a
// non-blocking miss.
const maxWaiterIDLen = 256

// ErrTooManyWaiters reports that a container lookup was shed because the
// store already holds the maximum number of blocked lookups. Callers should
// surface it as a retryable condition (HTTP 503), never as "not found".
var ErrTooManyWaiters = errors.New("too many blocked container lookups")

// Store is safe for concurrent use.
type Store struct {
	ttl time.Duration
	now func() time.Time

	// maxWaiters caps concurrently blocked GetContainer calls (see
	// defaultMaxWaiters); SetMaxWaiters overrides it (tests, tuning).
	maxWaiters int

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
	// specific ID becomes resolvable. nWaiters counts the channels across all
	// keys (bounded by maxWaiters).
	waiters  map[string][]chan struct{}
	nWaiters int
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
	// terminating is true once the pod has a deletionTimestamp (graceful
	// teardown in progress; phase stays Running). Such a pod's status still
	// carries its now-recycled PodIP, so it must not steal the IP index from a
	// live pod that legitimately holds it.
	terminating bool
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
		maxWaiters:  defaultMaxWaiters,
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
	pod, containers := kubeconvert.FromPod(p)

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
	rec.ownerRefs = cloneOwnerRefs(p.OwnerReferences)
	rec.resourceVersion = p.ResourceVersion
	rec.expireAt = time.Time{} // resurrect if a late update follows a delete
	rec.terminating = p.DeletionTimestamp != nil

	s.indexContainersLocked(rec, p.UID, containers, oldIDs)

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

	s.claimPodIPLocked(rec, pod, oldIP)
}

// indexContainersLocked replaces the record's container-ID index: new IDs are
// indexed (waking exactly the lookups blocked on them) and IDs the pod no
// longer reports are tombstoned for the TTL — they aged out of the kubelet's
// status (e.g. a second restart) but must stay resolvable.
func (s *Store) indexContainersLocked(rec *record, uid types.UID, containers map[string]kubemeta.Container, oldIDs map[string]struct{}) {
	ids := make(map[string]struct{}, len(containers))
	for id, c := range containers {
		ids[id] = struct{}{}
		s.byContainer[id] = &containerEntry{podUID: uid, container: c}
		// Wake exactly the requests blocked on this container ID.
		if ws := s.waiters[id]; len(ws) > 0 {
			for _, ch := range ws {
				close(ch)
			}
			s.nWaiters -= len(ws)
			delete(s.waiters, id)
		}
	}
	rec.containerIDs = ids
	for id := range oldIDs {
		if _, ok := ids[id]; ok {
			continue
		}
		if e := s.byContainer[id]; e != nil && e.podUID == uid && e.expireAt.IsZero() {
			s.expireEntryLocked(id, e)
		}
	}
}

// claimPodIPLocked maintains the live-pod IP index for one upsert: hostNetwork
// and finished pods never claim, a stale old mapping is dropped (identity-
// checked), and a TERMINATING claimant yields to a live incumbent — pod IPs
// recycle, and a drained pod's routine status updates still carry the IP the
// CNI already handed to someone else. Every live pod claims (last-write-wins),
// including a late-scheduled OLDER pod legitimately taking a freed IP.
func (s *Store) claimPodIPLocked(rec *record, pod kubemeta.Pod, oldIP string) {
	ip := pod.PodIP
	// The spec flag is the authoritative signal; the IP comparison stays as a
	// backstop for records converted before the field existed. Value equality
	// alone has a hole: an upsert carrying status.podIP before status.hostIP
	// is populated would let a hostNetwork pod claim the node IP.
	if pod.HostNetwork || ip == pod.HostIP {
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
		cur := s.byPodIP[ip]
		if cur == nil || cur == rec || !rec.terminating || cur.terminating {
			s.byPodIP[ip] = rec
		}
	}
}

// promoteIPClaimantLocked re-points byPodIP[ip] at a surviving eligible pod
// after the current claimant was deleted. Eligibility mirrors
// claimPodIPLocked: live (not tombstoned), running-phase, non-hostNetwork,
// status.podIP == ip. A non-terminating claimant is preferred over a
// terminating one (same precedence the claim path applies); among equals the
// pick is arbitrary — exactly like concurrent last-write-wins claims.
func (s *Store) promoteIPClaimantLocked(ip string) {
	var pick *record
	for _, r := range s.pods {
		if !r.expireAt.IsZero() { // tombstoned: not live
			continue
		}
		p := r.pod
		if p.PodIP != ip || p.HostNetwork || ip == p.HostIP || finishedPhase(p.Phase) {
			continue
		}
		if pick == nil || (pick.terminating && !r.terminating) {
			pick = r
		}
	}
	if pick != nil {
		s.byPodIP[ip] = pick
	}
}

// cloneOwnerRefs deep-copies owner references: the struct copy alone would
// alias the informer object's *bool fields (Controller, BlockOwnerDeletion),
// and stored records must share nothing with informer-owned memory.
func cloneOwnerRefs(refs []metav1.OwnerReference) []metav1.OwnerReference {
	if len(refs) == 0 {
		return nil
	}
	out := make([]metav1.OwnerReference, len(refs))
	for i, r := range refs {
		out[i] = r
		if r.Controller != nil {
			c := *r.Controller
			out[i].Controller = &c
		}
		if r.BlockOwnerDeletion != nil {
			b := *r.BlockOwnerDeletion
			out[i].BlockOwnerDeletion = &b
		}
	}
	return out
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
		// The deleted claimant may have been STALE: a force-deleted or
		// node-lost pod (never marked terminating) whose last-write-wins
		// claim shadowed the live holder. Promote a surviving eligible pod —
		// without this the live owner stays unresolvable until its next real
		// update (a same-RV resync short-circuits before re-claiming). The
		// scan only runs when the deleted pod owned an IP mapping, and only
		// walks the map once (deletes are informer-rate).
		s.promoteIPClaimantLocked(rec.pod.PodIP)
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
	// Only entries with NO expiry yet are stamped: a replayed DeletePod (an
	// informer resync) extends the pod tombstone but deliberately not the
	// container entries — their clocks started at the first deletion (or at a
	// restart replacement), and the invariant only requires containers to
	// expire NO LATER than their pod, which re-stamping the pod preserves.
	for id := range rec.containerIDs {
		if e := s.byContainer[id]; e != nil && e.podUID == uid && e.expireAt.IsZero() {
			e.expireAt = rec.expireAt
		}
	}
}

// finishedPhase reports whether a pod phase means the pod has stopped
// running (its IP is eligible for reuse by the CNI).
func finishedPhase(phase string) bool {
	return phase == "Succeeded" || phase == "Failed"
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
