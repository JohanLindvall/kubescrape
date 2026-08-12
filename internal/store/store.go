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
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/peerip"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta/kubeconvert"
)

// The blocked-lookup cap is a MEMORY budget expressed in waiters.
//
// A parked GetContainer is not a map entry. The map entry is the cheapest part
// of it: what it holds for the whole wait budget is a parked HTTP handler — two
// goroutines and their stacks, the connection's read and write buffers, the
// request state (including the parsed header map) and one file descriptor.
//
// A count is the right MECHANISM, but only while each waiter costs about the
// same — and the sender picks part of that cost unless something takes it away.
// Measured against a real listener with lookups parked on
// `/v1/containers/{id}?wait=`, as retained HEAP after a GC plus a flat 16 KiB
// per waiter for the goroutines net/http parks with it. The stack is ALLOWED
// FOR rather than measured because the runtime pools freed stacks, so
// MemStats.StackInuse cannot be attributed to one measurement — the same
// 200-waiter poll reported between 1.1 and 10.5 KB per waiter while its heap
// figure moved by 532 B (internal/server's parkedStackAllowance carries the
// derivation; overstating it can only make the assertions stricter):
//
//	an agent's actual poll                                   30 KB
//	the worst shape it now admits, measured                  46 KB
//	that shape's BOUND: a poll plus one admitted head        47 KB   <- budgeted
//	a 16 KB URI of %-escapes, with the URI HELD               63 KB
//	the widest header block, with the whole head HELD        266 KB
//	the same at net/http's 1 MiB default                >= 4.09 MB
//
// The measured worst and the budgeted bound nearly coincide because they are
// the same case: the worst shape is the one that gets a whole admitted head
// retained, so the arithmetic and the measurement meet. That is the bound
// working, not a coincidence.
//
// The budgeted row is the ARITHMETIC, not the measurement: the measurement
// moves several KB run to run and with the toolchain, and what cannot move is
// that a parked lookup retains an ordinary poll plus, at most, one copy of the
// head that was admitted.
//
// That is the whole reason WaiterCostBytes cannot be derived from anything this
// package allocates. The bottom three rows are what internal/server had to fix
// and how: net/http's parse expands a request head by 20 to 30 times, and most
// of that expansion cannot be counted from the request it hands the handler (it
// deletes Host/Transfer-Encoding/Trailer from the map, and net/textproto
// pre-sizes the map from a peek at the LINE count, so the capacity outlives the
// deletions; and the request LINE is one string that r.Method, r.RequestURI and
// r.URL.RawPath are all SLICES of, with two more unescaped copies made for a
// path carrying a %-escape). So the head is RELEASED before the handler parks
// (internal/server's releaseParkedHead), which makes the parked cost a property
// of this process rather than of the request.
// internal/server/waitercost_test.go re-measures it against a real listener and
// fails if it outgrows WaiterCostBytes.
//
// What is left is ONE copy of the wire, which is why the budget can be stated
// at all: whatever the sender spends its head on, the retained residue is
// either the request line (which http.conn.lastMethod holds for the life of the
// connection, out of any handler's reach) or the If-None-Match validator (kept
// on purpose), and they share the one admission rather than adding to it. That
// admission is ~16 KB, not the 8 KiB MaxHeaderBytes nor the 12 KiB a fresh
// connection is held to: a head arriving on a REUSED connection is partly
// pre-buffered by the previous request's read and never charged (measured
// 16350 admitted, 16351 refused; internal/server's maxHeaderBytes carries the
// mechanism, and its waitercost_test.go re-derives the number).
//
// The cap then has to be CHOSEN against that cost, and the historical 16384 was
// not: the shipped chart requests 128Mi for this pod and sets no limit
// (charts/kubescrape/values.yaml), so 16384 permitted several times the pod's
// entire request in parked requests alone — node memory pressure and an
// eviction, which is the outcome the cap exists to prevent, reached by way of
// the cap itself. So the default is a division:
//
//	WaiterBudgetBytes / WaiterCostBytes = 512 waiters
//
// What the cap costs when it binds: a LEGITIMATE blocked lookup is one node
// agent's tailer waiting out the ~1s gap between a container starting and the
// kubelet posting its ID. On the DEFAULT agent configuration there is at most
// one of those per node — the tailer resolves on ONE sweep goroutine, and the
// cadvisor path never waits. The exception is an agent run with
// `-ingest-metadata-wait` (default 0, which blocks not at all): its ingest
// handlers wait too, one lookup at a time per push, so that agent can add up to
// its own `-ingest-max-in-flight` (default 32) blocked lookups on top. So the
// default binds when hundreds of nodes sit in that window at the same instant —
// far sooner if the fleet waits on ingest — and what it costs there is bounded
// and retryable: 503 +
// Retry-After, counted kubescrape_container_lookups_shed_total, and the agent's
// metadata backoff re-asks. A few seconds of one file's log shipping, never
// data.
//
// A cluster big enough to reach it has also outgrown the 128Mi this budget is a
// quarter of — the records in THIS package measured 3.2 KB per pod (20k
// two-container pods with ordinary labels, annotations and statuses), with the
// informer's own trimmed copy on top — so raising it is one
// half of a pair: `-max-blocked-lookups n` on the service, and n x
// WaiterCostBytes more memory on the pod. That flag, not this constant, is the
// knob; the constant only says what the DEFAULT pod can afford.
const (
	// WaiterCostBytes is what one parked lookup is budgeted at. The worst case
	// admissible today is 47 KB — an ordinary poll plus one copy of the ~16 KB
	// head a reused connection admits, whichever term the sender spends it on —
	// against 30 KB for an ordinary agent poll, both on a keep-alive connection,
	// which is the shape every agent's net/http.Transport uses and the shape
	// that admits the most wire (the worst shape MEASURES 46 KB; 47 is the bound
	// it cannot pass). This covers the worst with the ~1.2x
	// RSS overhead measured alongside it (RSS is what gets a pod evicted) and
	// then some. The headroom is deliberately not spent: the worst case was
	// 51 KB when this number was chosen, and re-dividing the budget on every
	// measurement would move -max-blocked-lookups' default — which is a
	// documented operational number — for a memory saving nobody asked for.
	WaiterCostBytes = 64 << 10
	// WaiterBudgetBytes is what parked lookups on an unauthenticated route may
	// occupy in total: a quarter of the 128Mi the chart requests for this pod,
	// leaving the store, the informer caches and Go's own slack the rest. In
	// TOTAL means both parking spots — this package's per-ID waiters and
	// internal/server's readiness wait, which draws on the same cap through
	// TryPark; a budget covering one of the two would be spent twice.
	WaiterBudgetBytes = 32 << 20
	// DefaultMaxWaiters bounds concurrently blocked container lookups — the
	// waiters here and the readiness parks together — unless
	// -max-blocked-lookups overrides it.
	DefaultMaxWaiters = WaiterBudgetBytes / WaiterCostBytes
)

// maxWaiterIDLen bounds the container-ID strings held as waiter keys
// (kubemeta.MaxContainerIDLen carries the 64-hex-runtime rationale). Lookups
// over the bound degrade to a non-blocking miss — never an error, and never a
// pinned map entry.
//
// It bounds LENGTH, which is not the whole of what a key can cost: a short id
// CUT OUT of a long string (a slice, which in Go keeps the whole backing array
// alive) pins that string for as long as the waiter lives. Callers that derive
// an id from request text must copy it, which is what internal/server's
// handleContainer does before this package ever sees it.
const maxWaiterIDLen = kubemeta.MaxContainerIDLen

// ErrTooManyWaiters reports that a container lookup was shed because the
// store already holds the maximum number of blocked lookups. Callers should
// surface it as a retryable condition (HTTP 503), never as "not found".
var ErrTooManyWaiters = errors.New("too many blocked container lookups")

// ErrShuttingDown reports that a blocking container lookup was refused because
// Drain has been called: the process is terminating and the metadata this
// lookup waits for can no longer arrive. Callers surface it exactly like
// ErrTooManyWaiters — a retryable 503, never a 404 — because the container may
// well exist and the next pod behind the Service can answer.
var ErrShuttingDown = errors.New("store is shutting down")

// Store is safe for concurrent use.
type Store struct {
	ttl time.Duration
	now func() time.Time

	// maxWaiters caps concurrently blocked container lookups — the GetContainer
	// waiters below and the caller's readiness parks (TryPark) together — as a
	// memory budget expressed in waiters, see DefaultMaxWaiters; SetMaxWaiters
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
	// pod.DeletionTimestamp. Such a pod's status still carries its
	// now-recycled PodIP, so it must not steal the IP index from a live pod
	// that legitimately holds it.
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
func New(ttl time.Duration) *Store {
	return &Store{
		ttl:         ttl,
		now:         time.Now,
		maxWaiters:  DefaultMaxWaiters,
		pods:        make(map[types.UID]*record),
		byContainer: make(map[string]*containerEntry),
		byNode:      make(map[string]map[types.UID]*record),
		byPodName:   make(map[string]*record),
		byPodIP:     make(map[string]*record),
		ipClaimants: make(map[string]map[string]*record),
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
		s.deletePodLocked(types.UID(prev.pod.UID))
	}
	s.byPodName[nameKey] = rec

	s.claimPodIPLocked(rec, pod, oldIPs)
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
func (s *Store) claimPodIPLocked(rec *record, pod kubemeta.Pod, oldIPs []string) {
	// EVERY address the pod reports. On a dual-stack cluster a connection can
	// arrive from the family status.podIP does not carry, and indexing only
	// that one left /v1/self and /v1/pod-ips unresolvable for it — the agent's
	// self-attribution and the ingest peer-IP fallback silently off, with a 404
	// indistinguishable from any other.
	addrs := podAddresses(pod)
	for _, ip := range addrs {
		s.claimOneIPLocked(rec, ip, oldIPs)
	}
	// Addresses the pod no longer reports are released.
	for _, old := range oldIPs {
		if old == "" || containsStr(addrs, old) {
			continue
		}
		s.releaseIPLocked(rec, old)
	}
}

// rawIPs is the ONE enumeration of the addresses a pod's status reports:
// status.podIPs, falling back to the single status.podIP when the list is
// empty (kubeconvert fills PodIPs today; the backstop covers records built
// before the field existed). Both podAddresses (claim eligibility) and
// recordAddresses (release/cleanup) MUST read this same list — a claim taken
// from an address one enumeration sees and the other does not is never
// released, the dual-stack leak class deletePodLocked's comment describes.
//
// Every address is CANONICALISED, because this is what the index is keyed by
// while every lookup arrives in peerip's form: /v1/self from the connection's
// source address, the agent's ingest fallback through metaclient.PodByIP. A
// kubelet or CNI reporting `::ffff:10.1.2.3`, `FD00::0:7` or a zoned address
// would otherwise index the pod under a key no lookup can ever form, and both
// of those paths would 404 for it — indistinguishable from any other miss. The
// SERVED model keeps the verbatim strings; only the keys are normalised.
func rawIPs(pod kubemeta.Pod) []string {
	ips := pod.PodIPs
	if len(ips) == 0 && pod.PodIP != "" {
		ips = []string{pod.PodIP}
	}
	out := make([]string, len(ips))
	for i, ip := range ips {
		out[i] = peerip.Canonical(ip)
	}
	return out
}

// podAddresses returns the addresses a pod may legitimately be reached at, or
// nil when it claims none (hostNetwork, finished).
func podAddresses(pod kubemeta.Pod) []string {
	// The spec flag is the authoritative signal; the IP comparison stays as a
	// backstop for records converted before the field existed.
	if pod.HostNetwork || kubemeta.FinishedPhase(pod.Phase) {
		return nil
	}
	ips := rawIPs(pod)
	// The host address in the same form the pod addresses are keyed in, or the
	// comparison below misses whenever the two are spelled differently.
	host := peerip.Canonical(pod.HostIP)
	out := ips[:0:0]
	for _, ip := range ips {
		// A hostNetwork pod whose status.hostIP has not been populated yet
		// would otherwise claim the node address.
		if ip != "" && ip != host {
			out = append(out, ip)
		}
	}
	return out
}

// recordAddresses returns every address a record ever claimed, including for a
// pod that has since gone finished or hostNetwork (podAddresses returns none
// for those, but their claims still have to be cleaned up).
func recordAddresses(rec *record) []string {
	return rawIPs(rec.pod)
}

func containsStr(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// releaseIPLocked drops rec's claim on one address and promotes a survivor.
func (s *Store) releaseIPLocked(rec *record, ip string) {
	s.dropClaimantLocked(ip, rec)
	if s.byPodIP[ip] == rec {
		delete(s.byPodIP, ip)
		// A live pod shadowed on that address by the recycle race must be
		// promoted, or it stays unresolvable until its own next real upsert.
		s.promoteIPClaimantLocked(ip, rec)
	}
}

// claimOneIPLocked applies the claim rules for ONE of a pod's addresses.
//
// It deliberately takes no kubemeta.Pod: eligibility and precedence come from
// RECORD state the caller has already established (rec.terminating, rec.ipSeq),
// not from the pod value. Passing the pod invited a later edit to re-derive
// them here and bypass the ipSeq ordering.
func (s *Store) claimOneIPLocked(rec *record, ip string, oldIPs []string) {
	s.addClaimantLocked(ip, rec)
	{
		if !containsStr(oldIPs, ip) {
			// This pod ACQUIRED the address now. The sequence orders genuine
			// acquisitions so a later one beats an earlier one below.
			s.ipSeq++
			rec.ipSeq = s.ipSeq
		}
		cur := s.byPodIP[ip]
		switch {
		case cur == nil || cur == rec:
			s.byPodIP[ip] = rec
		case rec.terminating && !cur.terminating:
			// A draining pod keeps reporting its now-recycled IP; it yields.
		case !rec.terminating && cur.terminating:
			s.byPodIP[ip] = rec
		case rec.ipSeq > cur.ipSeq:
			// Last acquisition wins — including a late-scheduled older pod
			// legitimately taking a freed address.
			s.byPodIP[ip] = rec
		default:
			// rec is merely RE-ASSERTING an address it already held while a
			// later pod legitimately took it. Plain last-write-wins let any
			// unrelated update to a stale pod (a node-lifecycle condition on a
			// NotReady node, a resurrect after DeletePod, a transient podIP
			// blip) steal the mapping from the live owner and mis-attribute
			// every peer-IP lookup until that pod finally went away.
		}
	}
}

// promoteIPClaimantLocked re-points byPodIP[ip] at a surviving eligible pod
// after the current claimant released or lost the IP. Eligibility mirrors
// claimPodIPLocked, through the same podAddresses helper: live (not
// tombstoned), running-phase, non-hostNetwork, and holding ip among
// status.podIPs — the SECONDARY address of a dual-stack pod included. A non-terminating claimant is preferred over a
// terminating one (same precedence the claim path applies); among equals the
// pick is arbitrary — exactly like concurrent last-write-wins claims.
//
// skip is the record that just gave the IP up and must never win it back. It
// is REQUIRED for correctness on the delete path: DeletePod stamps expireAt
// (the tombstone marker this scan filters on) only AFTER the promotion runs,
// and with -cache-ttl 0 removes the record instead of stamping it at all — so
// without an explicit exclusion the pod being deleted is still "live" here and
// re-claims its own IP. That resurrects it in byPodIP with DeletedAt unset,
// serving a DELETED pod from GET /v1/pod-ips forever (Sweep never revisits
// byPodIP), leaking one entry per deleted pod, and — when a live pod holds the
// recycled IP — stealing the mapping from the real owner at map-iteration
// random.
func (s *Store) promoteIPClaimantLocked(ip string, skip *record) {
	var pick *record
	for _, r := range s.ipClaimants[ip] {
		if r == skip { // released the IP; never a candidate to re-take it
			continue
		}
		if !r.expireAt.IsZero() { // tombstoned: not live
			continue
		}
		// Eligibility through the SAME helper the claim path uses: it returns
		// the addresses a pod may legitimately hold and nothing for a
		// hostNetwork or finished one, so this subsumes the four separate
		// checks that stood here. They compared the single p.PodIP, which
		// meant a dual-stack claimant could never be promoted onto its
		// SECONDARY address — the address then resolved to nothing at all.
		if !containsStr(podAddresses(r.pod), ip) {
			continue
		}
		if pick == nil || beatsClaimant(pick, r) {
			pick = r
		}
	}
	if pick != nil {
		s.byPodIP[ip] = pick
	}
}

// beatsClaimant reports whether candidate should displace cur for an address.
// It is the claim path's precedence (claimPodIPLocked): a live pod beats a
// terminating one, then the LATER acquisition wins. Promotion used only the
// terminating half, so between two live claimants the winner was map-iteration
// random — reintroducing, on this one path, the stale-pod-wins outcome ipSeq
// exists to prevent.
func beatsClaimant(cur, candidate *record) bool {
	if cur.terminating != candidate.terminating {
		return cur.terminating
	}
	return candidate.ipSeq > cur.ipSeq
}

// addClaimantLocked records that rec currently reports ip.
func (s *Store) addClaimantLocked(ip string, rec *record) {
	m := s.ipClaimants[ip]
	if m == nil {
		m = make(map[string]*record, 1)
		s.ipClaimants[ip] = m
	}
	m[rec.pod.UID] = rec
}

// dropClaimantLocked forgets rec's claim on ip, removing the address's entry
// once nobody reports it (the map must not grow by one key per recycled IP).
func (s *Store) dropClaimantLocked(ip string, rec *record) {
	m := s.ipClaimants[ip]
	if m == nil {
		return
	}
	if cur, ok := m[rec.pod.UID]; ok && cur == rec {
		delete(m, rec.pod.UID)
	}
	if len(m) == 0 {
		delete(s.ipClaimants, ip)
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
		return
	}
	now := s.now()
	s.removeFromNodeLocked(rec.pod.NodeName, uid)
	// EVERY address, through the one helper that drops the claim, deletes the
	// mapping under an identity check and promotes a survivor.
	//
	// Releasing only rec.pod.PodIP from byPodIP left a dual-stack pod's
	// SECONDARY entry pointing at the deleted record for the process lifetime
	// (Sweep never revisits byPodIP): the whole kubemeta.Pod stayed reachable,
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

// Stats reports current cache sizes.
func (s *Store) Stats() (pods, containers int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pods), len(s.byContainer)
}

// expired reports whether a tombstone stamp has lapsed. A zero expireAt is
// the live marker (no tombstone), never "expired at the epoch"; a stamped
// entry expires strictly AFTER its instant, so an injected test clock sitting
// exactly on the stamp still resolves. Every expiry decision — the lookups'
// present-but-unswept checks and Sweep itself — goes through here so they
// cannot disagree on either edge.
func expired(expireAt, now time.Time) bool {
	return !expireAt.IsZero() && now.After(expireAt)
}

// Sweep removes expired tombstones. It is exported for tests; Run calls it
// periodically.
func (s *Store) Sweep() {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, e := range s.byContainer {
		if expired(e.expireAt, now) {
			delete(s.byContainer, id)
		}
	}
	for uid, rec := range s.pods {
		if !expired(rec.expireAt, now) {
			continue
		}
		s.dropNameIndexLocked(rec)
		for _, ip := range recordAddresses(rec) {
			s.dropClaimantLocked(ip, rec)
		}
		// The record's OWN container IDs, identity-checked exactly as
		// deletePodLocked does — never a rescan of byContainer. Container
		// entries always expire no later than their pod (deletePodLocked stamps
		// them with the pod's expiry or earlier, and both loops here share one
		// `now`), so the rescan was documented as normally removing nothing —
		// but it ran a full second pass over every container in the store,
		// each entry paying a pods lookup, whenever ANY tombstone lapsed. In a
		// 30k-pod cluster ordinary churn puts one in essentially every sweep
		// window, and the pass runs under the exclusive write lock every lookup
		// and every targets request waits behind.
		for id := range rec.containerIDs {
			if e := s.byContainer[id]; e != nil && e.podUID == uid {
				delete(s.byContainer, id)
			}
		}
		delete(s.pods, uid)
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
