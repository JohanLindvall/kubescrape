package store

// Blocked container lookups: the memory budget that caps them, the parking
// slots (this store's per-ID waiters and the caller's readiness parks through
// TryPark), and how they are released — by the upsert that indexes the ID, or
// by Drain at shutdown.

import (
	"errors"
	"slices"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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

// WithMaxWaiters overrides the blocked-lookup cap — the production caller is
// cmd/kubescrape's -max-blocked-lookups; tests use it to make the cap reachable.
// 0 or negative sheds every blocking lookup.
//
// The cap is a MEMORY budget expressed in waiters (DefaultMaxWaiters carries the
// measurement): each admitted waiter is a parked HTTP handler — two goroutines,
// their stacks, the connection buffers, the request and an fd, measured at 30 KB
// for an agent's poll and bounded at 47 KB for the worst request internal/server
// admits — on a route nothing authenticates. It covers BOTH spots such a lookup
// parks in (see TryPark). Raising it spends n x WaiterCostBytes of the pod's
// memory, so the pod's memory goes up first.
func WithMaxWaiters(n int) Option { return func(s *Store) { s.maxWaiters = n } }

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
// The channels are closed AND their map entries deleted through the same
// wakeLocked the wakeup path uses: an upsert racing the drain must not close a
// channel twice.
func (s *Store) Drain() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return 0
	}
	s.draining = true
	released := 0
	for id := range s.waiters {
		released += s.wakeLocked(id) // deleting the key mid-range is legal
	}
	return released
}

// wakeLocked releases every lookup parked on one container ID and reports how
// many it released: each channel is closed, the key deleted and nWaiters
// charged back. It is the one wake, shared by the upsert that indexes the ID
// (indexContainersLocked) and by Drain — deleting the key is what makes a later
// wake of the same ID a no-op rather than a second close of a closed channel.
func (s *Store) wakeLocked(id string) int {
	ws := s.waiters[id]
	if len(ws) == 0 {
		return 0
	}
	for _, ch := range ws {
		close(ch)
	}
	s.nWaiters -= len(ws)
	delete(s.waiters, id)
	return len(ws)
}

func (s *Store) removeWaiter(id string, ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ws := s.waiters[id]
	for i, c := range ws {
		if c == ch {
			// slices.Delete zeroes the vacated tail slot, so the backing array
			// does not keep the removed channel reachable.
			s.waiters[id] = slices.Delete(ws, i, i+1)
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
// The COUNTED total (nWaiters+nExternal) never exceeds the cap: both doors
// refuse before they increment, under the mutex. The handover between them is
// deliberately not atomic — waitReady releases its slot before GetContainer
// takes one — so for one mutex acquisition a lookup in transit holds a handler
// but no slot, and the handlers actually pinned may exceed the cap by the number
// of lookups in that gap. It cannot grow: once the caches are synced nothing
// enters the readiness park at all. The lookup in transit is not guaranteed its
// slot back, either: if its ID is still unknown when it reaches the waiter table
// and a racing request took the freed slot, it is SHED like any other refusal
// (ErrTooManyWaiters, counted in kubescrape_container_lookups_shed_total) —
// possible only at the end of the initial sync, and only with the cap
// saturated at that instant.
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
