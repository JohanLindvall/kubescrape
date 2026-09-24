package server

// The lookup handlers for the v1 metadata endpoints — containers, pods (by
// name, UID and IP) and node metadata — plus the shared JSON response helpers
// and the wait budget. The other routes each have a file of their own:
// /v1/self (self.go), /v1/nodes/{node}/targets (nodetargets.go, with the
// accumulator in targetdedup.go and its warnings in targetwarn.go),
// /v1/scrape-auth (scrapeauth.go, auth.go) and /v1/explain (explain.go); the
// HTTP cache — ETags, max-age and the node-targets memo — is httpcache.go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// handleContainer serves GET /v1/containers/{id}?wait=2s.
//
// The ID may include the runtime prefix ("containerd://..."), URL-escaped or
// not. If the ID is unknown the request blocks up to the wait budget for the
// metadata to appear (covering the gap between container start and the API
// server reporting the container ID).
func (s *Server) handleContainer(w http.ResponseWriter, r *http.Request) {
	wait, err := s.waitBudget(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id := kubemeta.NormalizeContainerID(r.PathValue("id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "empty container id")
		return
	}
	if len(id) > kubemeta.MaxContainerIDLen {
		// A real runtime ID is 64 hex characters (kubemeta.MaxContainerIDLen
		// carries the rationale); a kilobytes-long path segment is hostile
		// input, not a container that might yet appear, so it 400s up front and
		// never reaches the store's waiter map.
		writeError(w, http.StatusBadRequest, "container id too long")
		return
	}
	// …and the id that passed that check is a SLICE of the path: NormalizeContainerID
	// returns what follows the last colon, so `<16 KB>:<64hex>` yields a 64-byte
	// string whose backing array is the whole 16 KB. It outlives the release
	// below — it is this handler's local for the whole wait AND the store's
	// waiter-map key — so it is copied out here, where the length check has just
	// bounded what the copy can cost.
	id = strings.Clone(id)

	// Everything this handler still needs from the request head has been read
	// above, and both of its parking spots are below. Drop the rest before
	// either: net/http's parse of a head expands it by 20-30x and the store's
	// waiter cap is a COUNT, so a request that holds its parsed head across the
	// wait budget makes that count bound a number rather than the process
	// (releaseParkedHead carries the measurements). "id" is the route's wildcard
	// (`GET /v1/containers/{id...}`), whose matched value is one of the copies.
	releaseParkedHead(r, "id")

	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()

	// The one Debug seam on this route, and the reason it is a seam rather
	// than a line per request: /v1/containers is polled by every agent for
	// every log file, so an unconditional entry line is a flood at Info cost.
	// slog evaluates arguments EAGERLY, so even the time.Now() below is behind
	// the level check; everything emitted from here reports a TRANSITION (a
	// lookup that actually blocked, or one that was refused), never the warm
	// path.
	debug := s.log().Enabled(ctx, slog.LevelDebug)
	var started time.Time
	if debug {
		started = time.Now()
	}

	// Don't report "not found" from a cache that hasn't finished its initial
	// sync; spend the wait budget on readiness first if needed. A drain ends
	// that wait too — the shutdown path must not leave a request parked here
	// (see Server.Drain), and its refusal carries Retry-After because the next
	// pod behind the Service can answer at once, unlike a sync that is merely
	// slow.
	if err := s.waitReady(ctx); err != nil {
		// errDraining: the next pod behind the Service can answer at once.
		// ErrTooManyWaiters: the blocked-lookup cap is saturated, exactly as it
		// is when the store refuses below — same refusal, same signal to the
		// agent's backoff. errNotSynced is the one that carries no Retry-After:
		// this pod is merely still filling its caches.
		if errors.Is(err, errDraining) || errors.Is(err, store.ErrTooManyWaiters) {
			w.Header().Set("Retry-After", "1")
		}
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	res, ok, err := s.store.GetContainer(ctx, id)
	if err != nil {
		// The store REFUSED to wait — the waiter cap is saturated
		// (ErrTooManyWaiters) or shutdown has drained the waiters
		// (ErrShuttingDown). Either way retryable, never a 404: the container
		// may exist momentarily, and on the shutdown path the next pod behind
		// the Service can answer at once.
		//
		// Both are counted (kubescrape_container_lookups_shed_total and
		// _drained_total), and the counters are what an alert reads; this line
		// is what an incident reads, because a shed storm's counter says how
		// many and never WHICH — and "the agent's first poll returned nothing"
		// is answered by seeing the ids that were refused.
		if debug {
			s.log().Debug("container lookup refused before it could wait",
				"id", id, "wait", wait, "error", err)
		}
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if debug {
		// A lookup that PARKED and one that hit the warm index are the same
		// call here — the store does not report which it did — so the elapsed
		// time is the discriminator, and blockedLookupFloor is what keeps the
		// warm path (microseconds) from emitting anything. Reporting per
		// transition rather than per request is the whole discipline: this
		// fires for a lookup that waited, whatever it then returned.
		if waited := time.Since(started); waited >= blockedLookupFloor {
			s.log().Debug("container lookup blocked and then woke",
				"id", id, "elapsed", waited.Round(time.Millisecond), "wait", wait, "found", ok)
		}
	}
	if !ok {
		// A lookup that BLOCKED for its whole budget and a lookup that missed
		// instantly are the same 404 to
		// kubescrape_http_requests_total{code="404"}, and they mean opposite
		// things (see obs.ContainerLookupTimeouts). DeadlineExceeded rather
		// than any ctx error: a client that hangs up mid-wait cancels, which
		// says nothing about the store, and wait>0 excludes the ?wait=0
		// pollers whose context is already expired on arrival.
		if wait > 0 && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			obs.ContainerLookupTimeouts.Inc()
			// Keyless: container ids churn, so a keyed table would fill with
			// keys that never repeat. The counter carries the rate and the
			// line carries one example to look up.
			if s.warnLookupTimeout.Allow(lookupTimeoutWarnEvery) {
				s.log().Warn("container lookup timed out: the id never appeared in the store within the wait budget",
					"id", id, "wait", wait,
					"note", "the agent retries, and the log lines of that container stay unattributed until it "+
						"resolves. A one-off is normal (the wait covers the gap between a container starting "+
						"and the kubelet posting its id, and a rotated file's id may never return); a sustained "+
						"rate means this replica's pod informer is not seeing those pods. Further reports are "+
						"suppressed for "+lookupTimeoutWarnEvery.String())
			}
		}
		writeError(w, http.StatusNotFound, fmt.Sprintf("container %q not found", id))
		return
	}
	s.enrich(&res.Pod, res.OwnerRefs)
	s.writeCached(w, r, kubemeta.ContainerMetadata{
		ContainerID: res.Container.ID,
		Container:   res.Container,
		Pod:         res.Pod,
	}, false)
}

// lookupTimeoutWarnEvery bounds the container-lookup timeout warning. Like
// every other repeating condition in this package it is a STATE — a pod the
// informer cannot see stays unseen — and the noticing code runs once per
// agent per file per retry, so an unthrottled line is a flood proportional to
// fleet size.
const lookupTimeoutWarnEvery = 5 * time.Minute

// blockedLookupFloor is the elapsed time above which a container lookup is
// reported (at Debug) as having BLOCKED. A warm hit is a read-locked map probe
// — microseconds — so anything past a millisecond either parked on the waiter
// channel or spent time in the readiness park, which are the two transitions
// worth a per-request line. The floor is deliberately generous: a busy
// scheduler can stretch a warm lookup, and a false line here is noise on the
// route with the highest request rate in the process.
const blockedLookupFloor = time.Millisecond

// servePod is the shared body of the pod endpoints: readiness gate, lookup,
// 404, owner/namespace enrichment, then the write its policy calls for.
// notFound is evaluated lazily so the success path never formats it.
func (s *Server) servePod(w http.ResponseWriter, r *http.Request, policy cachePolicy, lookup func() (store.NodePod, bool), notFound func() string) {
	if !s.requireReady(w, "") {
		return
	}
	np, ok := lookup()
	if !ok {
		// Errors are never cached: a 404 means "not attributable yet", and
		// holding onto it would delay the recovery it is waiting for. Said
		// explicitly where a heuristic could store it anyway — see noStore.
		if cc := policy.noStore(); cc != "" {
			w.Header().Set("Cache-Control", cc)
		}
		writeError(w, http.StatusNotFound, notFound())
		return
	}
	s.enrich(&np.Pod, np.OwnerRefs)
	if policy == cacheNone {
		w.Header().Set("Cache-Control", policy.noStore())
		s.writeJSON(w, http.StatusOK, np.Pod)
		return
	}
	s.writeCached(w, r, np.Pod, policy == cachePrivate)
}

// handlePod serves GET /v1/pods/{namespace}/{name}: full metadata for one
// pod looked up by name (used by the agent to attribute cadvisor series).
// Deleted pods stay resolvable until their tombstone expires.
func (s *Server) handlePod(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	s.servePod(w, r, cacheShared,
		func() (store.NodePod, bool) { return s.store.GetPodByName(namespace, name) },
		func() string { return fmt.Sprintf("pod %s/%s not found", namespace, name) })
}

// handlePodByUID serves GET /v1/pod-uids/{uid}: full metadata for one pod
// looked up by UID (used by the OTLP ingest enricher to attribute pushed
// telemetry). Deleted pods stay resolvable until their tombstone expires.
func (s *Server) handlePodByUID(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	s.servePod(w, r, cacheShared,
		func() (store.NodePod, bool) { return s.store.GetPodByUID(uid) },
		func() string { return fmt.Sprintf("pod uid %q not found", uid) })
}

// handlePodByIP serves GET /v1/pod-ips/{ip}: the LIVE pod owning a pod IP
// (the agent's opt-in peer-IP attribution for pushed OTLP). Deleted pods and
// hostNetwork pods never resolve.
func (s *Server) handlePodByIP(w http.ResponseWriter, r *http.Request) {
	ip := r.PathValue("ip")
	s.servePod(w, r, cacheNone, // see cacheNone: recycled IPs need immediacy
		func() (store.NodePod, bool) { return s.store.GetPodByIP(ip) },
		func() string { return fmt.Sprintf("no live pod with IP %q", ip) })
}

// handleNodeMetadata serves GET /v1/nodes/{node}/metadata: the node's
// labels and annotations (used by the agent for node-level attributes).
func (s *Server) handleNodeMetadata(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, "") {
		return
	}
	node := r.PathValue("node")
	meta := s.resolver.Node(node)
	if meta == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("node %q not found", node))
		return
	}
	s.writeCached(w, r, kubemeta.NodeMetadata{Name: node, ObjectMeta: *meta}, false)
}

// waitBudget determines how long a container lookup may block: MaxWait by
// default, optionally shortened by ?wait= (a Go duration or plain seconds).
func (s *Server) waitBudget(r *http.Request) (time.Duration, error) {
	v := r.URL.Query().Get("wait")
	if v == "" {
		return s.maxWait, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		secs, ierr := strconv.Atoi(v)
		if ierr != nil {
			return 0, fmt.Errorf("invalid wait parameter %q: use a duration like 2s", v)
		}
		// Reject negatives BEFORE the multiplication: a large-enough negative
		// (?wait=-9223372037) overflows time.Duration(secs)*time.Second and
		// wraps POSITIVE, slipping past the d < 0 check below — inconsistent
		// with the pinned negatives-are-rejected invariant even though the
		// clamp bounds it. Then guard positive overflow, and let the shared
		// duration clamp below apply — clamping by TRUNCATED whole seconds here
		// would turn a sub-second maxWait into 0 (non-blocking) for ?wait=1.
		if secs < 0 {
			return 0, errors.New("wait parameter must not be negative")
		}
		if secs > int(math.MaxInt64/int64(time.Second)) {
			d = s.maxWait
		} else {
			d = time.Duration(secs) * time.Second
		}
	}
	if d < 0 {
		return 0, errors.New("wait parameter must not be negative")
	}
	if d > s.maxWait {
		d = s.maxWait
	}
	return d, nil
}

// reportEncodeFailure reports a response this process could not serialise.
// what names the response; outcome says what the client got, and is one of the
// two constants below: writeCached and handleNodeTargets marshal BEFORE writing
// anything and answer 500, while writeJSON streams and has already sent its
// status line, so its client got a truncated body instead.
//
// It is an ERROR and it is a bug: every value written here is a kubemeta
// document built from informer objects, holding nothing encoding/json refuses.
// If it ever happens it happens for EVERY request on that route — a permanent
// 500 (or truncated 200) whose only other trace is
// kubescrape_http_requests_total — so it is throttled rather than
// unconditional, and the throttle is keyless because one broken document
// breaks its whole route. ONE reporter and one throttle for both outcomes: it
// is one condition, and a second, package-level reporter logging through
// slog.Default (the one writeJSON had while it was a package function) bypassed
// the Server's own logger and shared its throttle across every Server in a
// process.
func (s *Server) reportEncodeFailure(what, outcome string, err error) {
	if s.warnEncode.Allow(encodeWarnEvery) {
		s.log().Error("encoding a response failed"+outcome,
			"what", what, "error", err,
			"note", "this cannot happen for a well-formed metadata document; further reports are suppressed for "+
				encodeWarnEvery.String())
	}
}

// The two outcomes reportEncodeFailure distinguishes; see it.
const (
	encodeAnswered500 = ", so this route answers 500 until the offending object changes"
	encodeTruncated   = " after the status line was written, so the client received a truncated body"
)

// encodeWarnEvery bounds the encode-failure report; see reportEncodeFailure.
const encodeWarnEvery = 5 * time.Minute

// writeJSON streams v as the JSON body of a status response.
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// A failure here is almost always the CLIENT going away mid-write, which
	// is not this server's business and would flood on a rolling agent update.
	// The three errors that mean the VALUE cannot be encoded are a different
	// thing entirely — a bug that makes the route answer a truncated 200
	// forever — so those, and only those, are reported.
	if err := json.NewEncoder(w).Encode(v); err != nil && unencodable(err) {
		s.reportEncodeFailure("a streamed JSON response", encodeTruncated, err)
	}
}

// unencodable reports an error that names the VALUE rather than the connection:
// encoding/json returns these three before writing anything, so they are the
// ones that mean "this document can never be served".
func unencodable(err error) bool {
	var (
		unsupportedType  *json.UnsupportedTypeError
		unsupportedValue *json.UnsupportedValueError
		marshaler        *json.MarshalerError
	)
	return errors.As(err, &unsupportedType) || errors.As(err, &unsupportedValue) || errors.As(err, &marshaler)
}

// writeError answers status with {"error": msg}. It encodes directly rather
// than through s.writeJSON: a map[string]string cannot be unencodable, so the
// only error left is the client going away, which is not this server's to
// report — and needing no Server keeps the refusal paths that call it
// (writeUnauthorized, requireReady) simple.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
