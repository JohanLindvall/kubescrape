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

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// GetContainer looks up metadata by container ID (with or without the
// runtime scheme prefix). If the ID is not yet known it blocks until the
// metadata for that specific container arrives or ctx is done — waiting is
// per container ID, not global on the cache. The initial lookup always
// happens, so an already-expired ctx degrades to a non-blocking lookup.
//
// The returned error is non-nil only when the lookup was shed by the waiter
// cap (ErrTooManyWaiters); ok is false then. A plain miss is (false, nil).
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
		s.mu.Lock()
		res, ok, gone = s.lookupLocked(id)
		if ok || gone {
			s.mu.Unlock()
			return res, ok, nil
		}
		if s.nWaiters >= s.maxWaiters {
			// Load shedding: every additional waiter is a pinned handler
			// goroutine + map entry for the full wait budget. Fail fast and
			// retryable rather than degrading everyone.
			s.mu.Unlock()
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

// SetMaxWaiters overrides the blocked-lookup cap (primarily for tests; 0 or
// negative sheds every blocking lookup). Not safe to call concurrently with
// lookups.
func (s *Store) SetMaxWaiters(n int) { s.maxWaiters = n }

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

// waiterCount reports the blocked-lookup count (tests).
func (s *Store) waiterCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nWaiters
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
