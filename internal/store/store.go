// Package store maintains an in-memory view of pod and container metadata:
// pod records keyed by UID, indexed by container runtime ID, by node name, by
// namespace/name and by pod IP.
//
// The store is populated from a pod informer (initial LIST, then WATCH
// events). Lookups for container IDs that are not yet known can block until
// the metadata arrives. Metadata for deleted pods and for replaced container
// IDs (container restarts) is retained for a configurable TTL so that
// short-lived workloads can still be resolved shortly after they are gone.
package store

import (
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta/kubeconvert"
)

// Store is safe for concurrent use.
type Store struct {
	ttl time.Duration
	now func() time.Time

	// maxWaiters caps concurrently blocked container lookups — the GetContainer
	// waiters below and the caller's readiness parks (TryPark) together — as a
	// memory budget expressed in waiters, see DefaultMaxWaiters; WithMaxWaiters
	// overrides it (tests, tuning).
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
	// ipClaimants indexes EVERY record currently reporting a given pod IP,
	// not just the winner in byPodIP. Promotion after a release needs the
	// other claimants for that one address; finding them by scanning every pod
	// cost O(pods) under the exclusive write lock on every pod lifecycle end
	// (measured: ~1 ms per delete at 30k pods, ~100% of DeletePod's cost),
	// on the single informer goroutine, while every reader — container, pod-uid,
	// pod-ip and node-target lookups — waits behind it. A rollout or CronJob
	// wave delivers those deletes in bursts, so "deletes are informer-rate"
	// held only in aggregate.
	ipClaimants map[string]map[string]*record
	// waiters holds blocked GetContainer calls keyed by the normalized
	// container ID they are waiting for; each channel is closed when that
	// specific ID becomes resolvable. nWaiters counts the channels across all
	// keys (bounded by maxWaiters).
	waiters  map[string][]chan struct{}
	nWaiters int
	// nExternal counts blocked container lookups parked OUTSIDE this store —
	// internal/server's waitReady, which holds the request for its whole wait
	// budget while the informer caches are unsynced. They cost the same parked
	// handler as a waiter here and they are the same route, so they spend the
	// same budget: the cap below is applied to nWaiters+nExternal, and TryPark
	// is how the other spot draws on it. Without that the cap covered whichever
	// spot happened to be in use, which is the wrong one — the readiness park is
	// STARTUP, when a whole agent fleet is likeliest to be asking at once.
	nExternal int
	// draining is set once by Drain and never cleared: after it, no lookup may
	// park, because the informers are stopping and the ID it would wait for can
	// never be indexed. A parked lookup that is not woken here is still parked
	// when the process exits — http.Server.Shutdown gives up WAITING at its
	// deadline but closes nothing — and the exit cuts the connection with no
	// status and no body, the one answer a client cannot act on.
	draining bool
	// ipSeq orders genuine pod-IP ACQUISITIONS so a later acquirer beats an
	// earlier one; a record merely re-asserting an address it already holds
	// keeps its old sequence and cannot displace the live owner.
	ipSeq uint64
	// pending lists every tombstone that has been stamped and not yet swept, so
	// a sweep costs what EXPIRED rather than what the store holds. See
	// stampLocked (the one place a stamp is made, and the invariant that keeps
	// this list complete) and sweep.
	pending []pendingExpiry

	// shed counts lookups refused by the waiter cap. Instance state published
	// through obs.RegisterWaiterStats rather than a counter this package bumps
	// itself — internal/store has no obs dependency, like the buffer stats and
	// the self-metadata gauge. Atomic because it is read outside the lock.
	shed atomic.Int64
	// drained counts lookups refused because the store is shutting down, kept
	// SEPARATE from shed: the cap binding means abuse or an anomaly, the drain
	// means a rolling update, and one counter for both would fire the first
	// alert on every deploy.
	drained atomic.Int64
	// nameReused counts pods that arrived under a namespace/name a DIFFERENT
	// live UID still held — the missed-delete guard in UpsertPod firing. It is
	// COUNTED rather than logged because the decision is made under the write
	// lock on the informer goroutine, where a log line would serialise every
	// reader behind formatting; the counter is published by
	// obs.RegisterStoreAnomalies, whose help carries what it means.
	nameReused atomic.Int64
	// ipContested counts pod-IP claims decided between two records that are
	// BOTH live — the recycle race the ipSeq ordering exists to resolve.
	// Terminating-vs-live hand-offs are the ORDINARY shape of a released
	// address and are deliberately not counted: they would swamp the signal
	// with the case that is already handled correctly by construction, and
	// what is worth seeing is two pods a lookup could legitimately have
	// confused. Same reason as nameReused for being a counter and not a log.
	ipContested atomic.Int64
	// gen is the store-wide change token: it advances whenever a mutation
	// lands that could alter what any read of this store returns. It is not
	// served on its own. nodeGen — what internal/server's node-targets ETag
	// memo reads, through NodeGeneration — is MINTED from it, which is what
	// keeps a node's stamp from ever repeating, and an EMPTY node answers with
	// it. Change tokens exist so a caller can prove a derived answer is still
	// current WITHOUT re-deriving it: before them that memo could only be
	// validated by a wall clock, so a conforming agent (whose poll interval
	// matches the max-age it was handed) never hit it and re-paid the whole
	// derivation and marshal on every poll.
	//
	// TWO ORDERING RULES, and both directions of getting them wrong are real
	// (they bind nodeGen too, which is stamped in the same bumpLocked):
	//
	//   - The WRITER bumps AFTER the mutation is visible, never before. Bumping
	//     first lets a reader observe the new token beside the OLD data and
	//     memoise a stale answer under a token that will never advance again —
	//     the one way this can serve wrong data rather than merely rebuild.
	//   - The READER loads the token BEFORE it reads the data. A change landing
	//     mid-derivation then leaves the memo tagged with the older token, so
	//     the next revalidation rebuilds. Conservative, which is the direction
	//     to be wrong in.
	//
	// It advances only on a REAL change: a resync that re-delivers a
	// byte-identical pod returns at resyncNoOp without touching it. That is
	// not an optimisation but the whole point — client-go re-delivers every
	// object each resync period, so bumping there would invalidate every memo
	// in the cluster on a timer and give back exactly what this buys.
	//
	// It is STORE-WIDE, which is too coarse to validate the node-targets memo
	// directly: that derivation reads only PodsOnNode(node), so when the memo
	// read this token it lapsed every node's memo on a pod event ANYWHERE in the
	// cluster (a readiness flip, a restart, a Job pod) and on every sweep that
	// removed a tombstone — the same trap an RV-keyed owner token would be.
	// Hence nodeGen, the per-node token the memo reads now.
	gen atomic.Uint64
	// nodeGen is the per-node change token NodeGeneration serves: for every
	// node with pods in byNode, the gen value minted by the last mutation that
	// changed that node's pod set. Stamped from gen (bumpLocked), so a value is
	// never reused — a node emptied and refilled gets a value larger than any
	// it had before — and it follows gen's two ordering rules, the stamp landing
	// under the same write lock as the mutation. An entry exists exactly while
	// byNode[node] does, so node churn cannot grow it; an EMPTY node answers
	// with the store-wide gen instead (see NodeGeneration for why that is
	// sound). sweep never stamps a node: a swept tombstone left byNode in
	// deletePodLocked, before it was ever a tombstone.
	nodeGen map[string]uint64
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
	// teardown in progress; phase stays Running), mirroring
	// pod.DeletionTimestamp. It does NOT order the IP index — a draining pod
	// still holds its address until its sandbox is torn down, so ipSeq alone
	// decides that (see claimOneIPLocked) — it marks the claims noteContested
	// excludes, an address changing hands from a drainer being the ordinary
	// release rather than a window in which a lookup was wrong.
	terminating bool
	// ipSeq is the store sequence at which this record last ACQUIRED an
	// address it did not already hold (see Store.ipSeq).
	//
	// Per-RECORD, not per-address: a dual-stack pod gaining a second address
	// also raises its precedence on the first. Observable only if a pod's
	// status gains an address while RETAINING a contested one — the kubelet
	// writes podIPs whole at sandbox start and does not emit that transition
	// — and making it exact costs a per-record map on the path every pod
	// upsert runs, so it is documented rather than paid for.
	ipSeq uint64
}

type containerEntry struct {
	podUID    types.UID
	container kubemeta.Container
	// expireAt is zero while the ID is currently reported by a live pod.
	expireAt time.Time
}

// New creates a store that retains metadata for deleted pods and replaced
// container IDs for ttl. A ttl <= 0 disables the tombstone cache.
//
// Everything tunable is set here (Option) rather than by a setter after
// construction, so there is no "before the first lookup" contract to keep.
func New(ttl time.Duration, opts ...Option) *Store {
	s := &Store{
		ttl:         ttl,
		now:         time.Now,
		maxWaiters:  DefaultMaxWaiters,
		pods:        make(map[types.UID]*record),
		byContainer: make(map[string]*containerEntry),
		byNode:      make(map[string]map[types.UID]*record),
		nodeGen:     make(map[string]uint64),
		byPodName:   make(map[string]*record),
		byPodIP:     make(map[string]*record),
		ipClaimants: make(map[string]map[string]*record),
		waiters:     make(map[string][]chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Option tunes a Store at construction (New).
type Option func(*Store)

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
	// The resync probe runs FIRST, under the read lock, because the expensive
	// part of this function is the conversion and not the indexing: FromPod
	// deep-copies the labels, annotations, owner refs and every container, and
	// with `-resync` set client-go re-delivers every pod in the cluster
	// byte-identical on each period. Converting first meant a 20k-pod resync
	// allocated 20k full copies to discard them on the next line, and took the
	// EXCLUSIVE lock 20k times — in a burst, against every reader and the
	// informer itself — to discover there was nothing to do.
	if s.resyncNoOp(p) {
		return
	}

	pod, containers := kubeconvert.FromPod(p)
	// The annotation budget's refusal, counted where the object's KIND is
	// known and once per informer EVENT — not once per served document, which
	// is the same pod re-marshalled on every agent poll. pkg/kubemeta cannot
	// import internal/obs, so each of the three doors counts its own kind (see
	// obs.MetadataAnnotationsOmitted); the served document is truthful on its
	// own through kubemeta.OmittedAnnotation.
	if kubemeta.AnnotationsOmitted(pod.Annotations) {
		obs.MetadataAnnotationsOmitted.WithLabelValues("Pod").Inc()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Deferred, so it runs AFTER the mutation below and BEFORE the unlock
	// (defers are LIFO) — the writer half of the ordering rule on gen. The
	// resyncNoOp return above is already behind us, so this is a real change;
	// the write-lock re-check below can still find nothing to do, and the
	// resulting spare bump costs one rebuild, which is the safe direction.
	// touched names the nodes whose pod set this upsert changed (the one the
	// pod left and the one it is on), filled in once they are known.
	var touched [2]string
	defer func() { s.bumpLocked(touched[:]...) }()

	rec := s.pods[p.UID]
	// Re-checked under the write lock, since the probe above dropped its lock:
	// the informer delivers pod events on one goroutine, so nothing can have
	// changed in between today, but that is the caller's property and not this
	// type's.
	if unchangedLocked(rec, p) {
		return // periodic resync, nothing changed
	}
	// A pod the store has never seen carries no evidence of WHEN it acquired
	// its addresses — see claimOneIPLocked, which is what reads this.
	firstSighting := rec == nil
	var oldNode string
	var oldIPs []string
	var oldIDs map[string]struct{}
	if rec == nil {
		rec = &record{}
		s.pods[p.UID] = rec
	} else {
		oldNode = rec.pod.NodeName
		// Every address the record previously claimed, so the ones this upsert
		// drops are released (a dual-stack pod has more than one).
		oldIPs = podAddresses(rec.pod)
		oldIDs = rec.containerIDs
	}

	rec.pod = pod
	rec.ownerRefs = cloneOwnerRefs(p.OwnerReferences)
	rec.resourceVersion = p.ResourceVersion
	rec.expireAt = time.Time{} // resurrect if a late update follows a delete
	// One source of truth for "this pod is draining": the converted model's
	// DeletionTimestamp, which is also what the API serves and what
	// scrape.Scrapeable filters on. Re-deriving it from p here would let the
	// two drift.
	rec.terminating = pod.DeletionTimestamp != nil

	s.indexContainersLocked(rec, p.UID, containers, oldIDs)

	touched = [2]string{oldNode, pod.NodeName}
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
	// A pod arriving under a name a DIFFERENT live UID still holds means the
	// old one is gone — its name has been reused, which for a StatefulSet
	// (stable names, fresh UID per recreation) is routine. Normally its own
	// Delete event handles that, and client-go synthesizes one from a
	// DeletedFinalStateUnknown tombstone even across a relist gap. But the
	// name index is the only place the collision is VISIBLE, and everything
	// else is keyed by UID: if that delete is ever missed, the old record
	// keeps its byNode entry and goes on being served as a live scrape target
	// forever, for a pod that no longer exists. Tombstone it here — the same
	// path a delete would take, so its containers stay resolvable for the TTL.
	nameKey := pod.Namespace + "/" + pod.Name
	if prev := s.byPodName[nameKey]; prev != nil && prev != rec && prev.expireAt.IsZero() {
		// Counted, because taking this branch is EVIDENCE that a Delete was
		// missed: the ordinary recreate order tombstones the predecessor
		// first, and expireAt is zero here. See the nameReused field.
		s.nameReused.Add(1)
		s.deletePodLocked(types.UID(prev.pod.UID))
	}
	s.byPodName[nameKey] = rec

	s.claimPodIPLocked(rec, pod, oldIPs, firstSighting && rec.terminating)
}

// resyncNoOp reports that this delivery carries a resourceVersion the store
// already holds for a LIVE record — an informer resync of an unchanged pod,
// which has nothing to do and nothing to convert. A tombstoned record is never
// a no-op: the upsert resurrects it.
func (s *Store) resyncNoOp(p *corev1.Pod) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return unchangedLocked(s.pods[p.UID], p)
}

// unchangedLocked is the ONE test both halves of UpsertPod's resync
// short-circuit apply (the read-locked probe and the write-locked re-check):
// the record is live and already holds this delivery's resourceVersion.
//
// An EMPTY resourceVersion is never "unchanged". The informer always sets one,
// so only a hand-built object (a test fixture, an embedder) lacks it, and for
// those two empty strings say nothing about the content — believing them
// silently dropped a re-upsert carrying a new pod IP or container ID.
// services.Index.Upsert applies the same rule, for the same reason.
func unchangedLocked(rec *record, p *corev1.Pod) bool {
	return rec != nil && rec.expireAt.IsZero() &&
		p.ResourceVersion != "" && rec.resourceVersion == p.ResourceVersion
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
		s.wakeLocked(id)
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

// NameReuses counts pods that arrived under a namespace/name a different live
// UID still held (see the nameReused field). Published through
// obs.RegisterStoreAnomalies.
func (s *Store) NameReuses() int64 { return s.nameReused.Load() }

// NodeGeneration is the change token for ONE node's pod set — what
// PodsOnNode(node) returns (see the nodeGen field). A caller holding a value
// derived from that set re-reads it and, if it is unchanged, knows its answer
// is still current without re-deriving it. Load it BEFORE reading the data it
// is meant to describe.
//
// A node with no pods answers with the STORE-WIDE token rather than a
// constant, and that is what keeps the answer sound across a node emptying and
// refilling: a node's stamp is minted by the mutation that last changed it, so
// the mutation that EMPTIES it advances gen past every stamp it ever had, and
// the one that refills it stamps past every gen value an empty read could have
// returned. A constant (0, say) would equate "empty before a pod arrived" with
// "empty again after it left", and a memo built across the first transition —
// token sampled empty, pod read non-empty — would then be served for the second.
func (s *Store) NodeGeneration(node string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if g, ok := s.nodeGen[node]; ok {
		return g
	}
	return s.gen.Load()
}

// bumpLocked is the writer half of the change tokens, called with the write
// lock held and AFTER the mutation (deferred, so before the caller's unlock):
// it advances the store-wide token and stamps every named node whose pod set
// the mutation changed with the value just minted. A node the mutation left
// with no pods loses its entry, which is what bounds nodeGen by the nodes that
// have pods.
func (s *Store) bumpLocked(nodes ...string) {
	g := s.gen.Add(1)
	for _, n := range nodes {
		if n == "" {
			continue
		}
		if _, ok := s.byNode[n]; ok {
			s.nodeGen[n] = g
		} else {
			delete(s.nodeGen, n)
		}
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
	s.deletePodLocked(uid)
}

func (s *Store) deletePodLocked(uid types.UID) {
	rec := s.pods[uid]
	if rec == nil {
		return // nothing changed, so the change token must not move
	}
	// After the mutation, before the caller's unlock; the pod's node is the one
	// whose PodsOnNode answer changes. The name-reuse path in UpsertPod comes
	// through here too, so the predecessor's node is stamped even when it is
	// not the node the new pod landed on.
	defer s.bumpLocked(rec.pod.NodeName)
	now := s.now()
	s.removeFromNodeLocked(rec.pod.NodeName, uid)
	// EVERY address, through the one helper that drops the claim, deletes the
	// mapping under an identity check and promotes a survivor.
	//
	// Releasing only rec.pod.PodIP from byPodIP left a dual-stack pod's
	// SECONDARY entry pointing at the deleted record for the process lifetime
	// (sweep never revisits byPodIP): the whole kubemeta.Pod stayed reachable,
	// and with -cache-ttl 0 — where deletePodLocked returns below WITHOUT
	// stamping DeletedAt — GetPodByIP's tombstone guard never fired, so a
	// deleted pod answered /v1/pod-ips and /v1/self on that address forever.
	// A live pod that legitimately took the address stayed unresolvable, since
	// nothing promoted it either.
	//
	// The promotion matters for the same reason it always did: the deleted
	// claimant may have been STALE (a force-deleted or node-lost pod, never
	// marked terminating) whose last-write-wins claim shadowed the live holder.
	for _, ip := range recordAddresses(rec) {
		s.releaseIPLocked(rec, ip)
	}

	if s.ttl <= 0 {
		s.removeRecordLocked(rec, uid)
		return
	}

	deletedAt := now
	rec.pod.DeletedAt = &deletedAt
	rec.expireAt = now.Add(s.ttl)
	s.stampLocked(pendingExpiry{when: rec.expireAt, uid: uid, isPod: true})
	// Only entries with NO expiry yet are stamped: a replayed DeletePod (an
	// informer resync) extends the pod tombstone but deliberately not the
	// container entries — their clocks started at the first deletion (or at a
	// restart replacement), and the invariant only requires containers to
	// expire NO LATER than their pod, which re-stamping the pod preserves.
	for id := range rec.containerIDs {
		if e := s.byContainer[id]; e != nil && e.podUID == uid && e.expireAt.IsZero() {
			e.expireAt = rec.expireAt
			// Listed even though the pod's own sweep takes its container IDs
			// with it: the pod may be RESURRECTED before that (a late update
			// after a missed delete) while an ID that has meanwhile aged out of
			// its status keeps this stamp. Then no pod sweep will ever reach
			// it, and the entry is only reclaimable through its own listing.
			s.stampLocked(pendingExpiry{when: e.expireAt, id: id})
		}
	}
}

// Stats reports current cache sizes.
func (s *Store) Stats() (pods, containers int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pods), len(s.byContainer)
}

// removeRecordLocked is the ONE teardown of a pod record, shared by the two
// paths that remove one outright — deletePodLocked with no tombstone cache
// (-cache-ttl <= 0) and sweepPodLocked retiring a lapsed tombstone: it drops the
// name index (unless a same-name successor already holds it), deletes the
// record's OWN container IDs — identity-checked, since a restart or a
// same-name successor may have re-indexed one under another pod, and never a
// rescan of byContainer — and removes the record. The three steps carry no
// ordering dependency on each other. IP claims are the caller's: the delete path
// releases them (promoting a survivor), the sweep path only forgets them.
func (s *Store) removeRecordLocked(rec *record, uid types.UID) {
	s.dropNameIndexLocked(rec)
	for id := range rec.containerIDs {
		if e := s.byContainer[id]; e != nil && e.podUID == uid {
			delete(s.byContainer, id)
		}
	}
	delete(s.pods, uid)
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
