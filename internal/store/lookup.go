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

	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/peerip"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// GetContainer looks up metadata by container ID (with or without the
// runtime scheme prefix). If the ID is not yet known it blocks until the
// metadata for that specific container arrives or ctx is done — waiting is
// per container ID, not global on the cache. The initial lookup always
// happens, so an already-expired ctx degrades to a non-blocking lookup.
//
// The returned error is non-nil only when the lookup was REFUSED rather than
// resolved: ErrTooManyWaiters (the waiter cap) or ErrShuttingDown (Drain has
// run). ok is false then, and both are retryable — a plain miss is (false, nil).
func (s *Store) GetContainer(ctx context.Context, id string) (ContainerResult, bool, error) {
	id = kubemeta.NormalizeContainerID(id)
	if id == "" {
		return ContainerResult{}, false, nil
	}
	// Fast path: read lock only.
	s.mu.RLock()
	res, ok, gone := s.lookupLocked(id)
	s.mu.RUnlock()
	if ok {
		return res, true, nil
	}
	if gone {
		// Expired tombstone: the container is definitively deleted, so
		// waiting for its metadata to (re)appear would just burn the budget.
		return ContainerResult{}, false, nil
	}
	if len(id) > maxWaiterIDLen {
		// Can never be a real runtime ID; do not hold client-chosen bytes as
		// a waiter key (memory amplification) — degrade to a plain miss.
		return ContainerResult{}, false, nil
	}
	for {
		// Double-checked: the ID may have been indexed since the read-locked
		// miss (e.g. every waiter waking at once); re-checking under the read
		// lock keeps such lookup bursts from serializing on the write lock.
		s.mu.RLock()
		res, ok, gone = s.lookupLocked(id)
		s.mu.RUnlock()
		if ok || gone {
			return res, ok, nil
		}
		if ctx.Err() != nil {
			// A lookup that cannot block must not pay for the waiter protocol:
			// registering takes the EXCLUSIVE lock, and removeWaiter takes it
			// again, for a channel the select below could never wait on. The
			// outcome is identical to the ctx.Done() arm — the read-locked probe
			// above already made the same final check that arm makes — but this
			// is the route's cheapest hostile shape (?wait=0, or a client that
			// hung up), and write-lock churn is contended by every reader and by
			// the informer goroutine.
			return ContainerResult{}, false, nil
		}
		s.mu.Lock()
		res, ok, gone = s.lookupLocked(id)
		if ok || gone {
			s.mu.Unlock()
			return res, ok, nil
		}
		if s.draining {
			// Shutting down: the informers are stopping, so the ID this lookup
			// would park on can never be indexed. Answer retryably instead of
			// holding the handler for a budget that outlives the process — the
			// exit is what would cut it, with no status at all. Checked BEFORE
			// the cap so a drain that arrives while the cap is full still
			// refuses for the honest reason.
			s.mu.Unlock()
			s.drained.Add(1)
			return ContainerResult{}, false, ErrShuttingDown
		}
		if s.nWaiters+s.nExternal >= s.maxWaiters {
			// Load shedding: every additional waiter is a pinned handler
			// goroutine + map entry for the full wait budget. Fail fast and
			// retryable rather than degrading everyone. The lookups parked in
			// the caller's readiness wait (TryPark) are counted here too: they
			// are the same handlers on the same route, so one budget covers
			// both spots.
			s.mu.Unlock()
			s.shed.Add(1)
			return ContainerResult{}, false, ErrTooManyWaiters
		}
		ch := make(chan struct{})
		s.waiters[id] = append(s.waiters[id], ch)
		s.nWaiters++
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			s.removeWaiter(id, ch)
			// The deadline and the wakeup can be ready simultaneously (select
			// picks arbitrarily): if the ID landed within the budget, serve it
			// rather than 404ing a request whose wait actually succeeded.
			s.mu.RLock()
			res, ok, _ = s.lookupLocked(id)
			s.mu.RUnlock()
			return res, ok, nil
		case <-ch:
			// The ID was indexed; loop to fetch it.
		}
	}
}

// SetMaxWaiters overrides the blocked-lookup cap — the production caller is
// cmd/kubescrape's -max-blocked-lookups; tests use it to make the cap reachable.
// 0 or negative sheds every blocking lookup. Not safe to call concurrently with
// lookups.
//
// The cap is a MEMORY budget expressed in waiters (DefaultMaxWaiters carries the
// measurement): each admitted waiter is a parked HTTP handler — two goroutines,
// their stacks, the connection buffers, the request and an fd, measured at 30 KB
// for an agent's poll and bounded at 47 KB for the worst request internal/server
// admits — on a route nothing authenticates. It covers BOTH spots such a lookup
// parks in (see TryPark). Raising it spends n x WaiterCostBytes of the pod's
// memory, so the pod's memory goes up first.
func (s *Store) SetMaxWaiters(n int) { s.maxWaiters = n }

// Drain releases every parked container lookup so its handler can answer, and
// refuses every later one, reporting how many were released. Idempotent.
//
// It is the shutdown counterpart of the wakeup in indexContainersLocked, and it
// must run BEFORE http.Server.Shutdown: Shutdown waits for the in-flight
// handlers and, at its deadline, simply RETURNS (it never closes an active
// connection — only srv.Close does). A lookup parked on a 30s wait is therefore
// still parked when the process exits a moment later, and the exit is what cuts
// it: no status, no body ("Empty reply from server"), the one outcome a client
// can neither retry on nor diagnose. Woken lookups re-check the index (an ID
// that landed in the same moment is still served) and otherwise return
// ErrShuttingDown, which the handler surfaces as 503 + Retry-After.
//
// The channels are closed AND their map entries deleted, exactly as the wakeup
// path does: an upsert racing the drain must not close a channel twice.
func (s *Store) Drain() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return 0
	}
	s.draining = true
	released := s.nWaiters
	for id, ws := range s.waiters {
		for _, ch := range ws {
			close(ch)
		}
		delete(s.waiters, id)
	}
	s.nWaiters = 0
	return released
}

func (s *Store) removeWaiter(id string, ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ws := s.waiters[id]
	for i, c := range ws {
		if c == ch {
			s.waiters[id] = append(ws[:i], ws[i+1:]...)
			s.nWaiters--
			break
		}
	}
	if len(s.waiters[id]) == 0 {
		delete(s.waiters, id)
	}
}

// TryPark reserves one slot of the blocked-lookup budget for a container lookup
// that parks OUTSIDE this store, and reports whether it may park. The caller
// MUST pair a true return with Unpark.
//
// The one caller is internal/server's waitReady, which holds a container lookup
// for its whole wait budget while the informer caches are still syncing. That
// park costs exactly what a waiter here costs (WaiterCostBytes: a pinned
// handler, its goroutines, the connection and an fd) on exactly the same
// unauthenticated route, so it draws on the same cap rather than on a second
// one — otherwise -max-blocked-lookups would bound half the requests it names,
// and the uncovered half is the STARTUP half, when a fleet of agents is
// likeliest to be asking at once.
//
// A refusal is counted as a shed lookup, like the store's own: it is the same
// cap binding for the same reason, and an operator watching
// kubescrape_container_lookups_shed_total must not have to know which of the two
// spots a request had reached.
//
// The handover is deliberately not atomic — waitReady releases its slot before
// GetContainer takes one — so the instantaneous total may exceed the cap by the
// number of lookups between the two, for as long as a mutex acquisition takes.
// It cannot grow: once the caches are synced nothing enters the readiness park
// at all.
func (s *Store) TryPark() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nWaiters+s.nExternal >= s.maxWaiters {
		s.shed.Add(1)
		return false
	}
	s.nExternal++
	return true
}

// Unpark returns a slot taken by TryPark.
func (s *Store) Unpark() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nExternal > 0 {
		s.nExternal--
	}
}

// BlockedLookups reports how many container lookups are blocked right now —
// both spots: this store's per-ID waiters and the lookups parked in the
// caller's readiness wait (TryPark). Published as a gauge (see
// obs.RegisterWaiterStats): it is what shows waiter pressure building BEFORE
// the cap starts shedding, and a gauge that omitted the readiness parks would
// read 0 through the whole window in which they are the only thing parked.
func (s *Store) BlockedLookups() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nWaiters + s.nExternal
}

// ShedLookups reports how many blocking lookups the waiter cap has refused
// since startup.
func (s *Store) ShedLookups() int64 { return s.shed.Load() }

// DrainedLookups reports how many blocking lookups have been refused because
// the store is shutting down (see Drain). Separate from ShedLookups: this one
// is expected to move on every rolling update.
func (s *Store) DrainedLookups() int64 { return s.drained.Load() }

// waiterCount reports the blocked-lookup count (tests).
func (s *Store) waiterCount() int { return s.BlockedLookups() }

// lookupLocked resolves a normalized container ID. gone reports an expired
// (present-but-unswept) entry — a deleted pod's tombstone or a
// restart-replaced container ID of a still-live pod; either way the ID can
// never reappear, so callers must not block waiting for it.
func (s *Store) lookupLocked(id string) (res ContainerResult, ok, gone bool) {
	e := s.byContainer[id]
	if e == nil {
		return ContainerResult{}, false, false
	}
	now := s.now()
	if expired(e.expireAt, now) {
		return ContainerResult{}, false, true
	}
	rec := s.pods[e.podUID]
	if rec == nil || expired(rec.expireAt, now) {
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
	if rec == nil || expired(rec.expireAt, s.now()) {
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
	if rec == nil || expired(rec.expireAt, s.now()) {
		return NodePod{}, false
	}
	return NodePod{Pod: rec.pod, OwnerRefs: rec.ownerRefs}, true
}

// GetPodByIP returns the live pod owning the given pod IP, if any. Deleted
// and finished pods never resolve (their IP may already belong to a new
// pod), and hostNetwork pods are not indexed.
//
// The argument is canonicalised through the same function the index keys on
// (peerip.Canonical), so a caller spelling an address the way its transport
// handed it over — a URL path value, an IPv4-mapped or uppercase IPv6 form —
// looks the pod up under the key the kubelet's own spelling produced.
func (s *Store) GetPodByIP(ip string) (NodePod, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec := s.byPodIP[peerip.Canonical(ip)]
	if rec == nil || rec.pod.DeletedAt != nil || kubemeta.FinishedPhase(rec.pod.Phase) {
		return NodePod{}, false
	}
	return NodePod{Pod: rec.pod, OwnerRefs: rec.ownerRefs}, true
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
