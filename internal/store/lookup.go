package store

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/obs"
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
	// parkedAt is when this lookup first parked; zero while it never has. Only
	// a parked lookup is observed (noteWait), so the fast paths above and the
	// first pass below cost nothing.
	var parkedAt time.Time
	for {
		// Double-checked: the ID may have been indexed since the read-locked
		// miss (e.g. every waiter waking at once); re-checking under the read
		// lock keeps such lookup bursts from serializing on the write lock.
		s.mu.RLock()
		res, ok, gone = s.lookupLocked(id)
		s.mu.RUnlock()
		if ok || gone {
			noteWait(parkedAt, waitResolved)
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
			noteWait(parkedAt, waitTimeout)
			return ContainerResult{}, false, nil
		}
		s.mu.Lock()
		res, ok, gone = s.lookupLocked(id)
		if ok || gone {
			s.mu.Unlock()
			noteWait(parkedAt, waitResolved)
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
		if parkedAt.IsZero() {
			parkedAt = time.Now()
		}

		select {
		case <-ctx.Done():
			s.removeWaiter(id, ch)
			// The deadline and the wakeup can be ready simultaneously (select
			// picks arbitrarily): if the ID landed within the budget, serve it
			// rather than 404ing a request whose wait actually succeeded.
			s.mu.RLock()
			res, ok, _ = s.lookupLocked(id)
			s.mu.RUnlock()
			if ok {
				noteWait(parkedAt, waitResolved)
			} else {
				noteWait(parkedAt, waitTimeout)
			}
			return res, ok, nil
		case <-ch:
			// The ID was indexed; loop to fetch it.
		}
	}
}

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

// The outcomes kubescrape_container_lookup_wait_seconds is labelled by.
const (
	waitResolved = "resolved"
	waitTimeout  = "timeout"
)

// noteWait observes how long a lookup was parked, once it is answered. A zero
// parkedAt is a lookup that never parked, which is every warm-path answer and
// deliberately not observed: the histogram is the pod informer's lag behind
// the kubelet, and a hit costs no wait to lag.
func noteWait(parkedAt time.Time, outcome string) {
	if parkedAt.IsZero() {
		return
	}
	obs.ContainerLookupWait.WithLabelValues(outcome).Observe(time.Since(parkedAt).Seconds())
}
