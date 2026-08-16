// Package bearer is the one implementation of "a bearer token lives in a
// mounted file that rotates under us".
//
// Kubernetes projects rotated Secret/ServiceAccount contents into the mounted
// path, so every consumer of such a token has the same three needs: re-read it
// periodically rather than capturing it once, survive the transient read
// failure a swap can produce, and — on the receiving side — keep accepting the
// PREVIOUS value for a grace window so the two ends never have to flip in
// lockstep.
//
// There were five implementations of that, and they disagreed on the decision
// that matters. On a FAILED RE-READ two of them kept the last good token and
// two failed the operation outright (the OTLP exporter failed the whole export;
// the kubelet scraper had no cache at all and re-read the file on every single
// request, so a Secret swap could fail a scrape). The same survivable window
// was survivable in one place and fatal in another. Here it is decided once:
//
//	a re-read failure keeps the last good value and warns;
//	a failure with NO last good value is an error.
//
// A rotation puts the two ends out of step in BOTH directions, and each
// direction needs its own mechanism:
//
//   - the CLIENT still presents the OLD token, because it re-reads on its own
//     cadence. DefaultGrace covers that: the receiver keeps accepting the
//     predecessor for the whole window.
//   - the client already presents the NEW one, because IT re-read first. No
//     grace can cover that — a receiver cannot accept a token it has never
//     read — so the only remedy is for the receiver to read SOON. That is
//     DefaultRefreshInterval: Rotating re-reads on a one-second cadence rather
//     than DefaultReadInterval's minute, so the receiver's copy of a file it
//     shares with its clients is never more than a second behind theirs.
//
// The second half was missing, and only the first was documented. A rotation
// therefore 401'd every client that re-read before the receiver did — up to a
// full minute of hard rejections on /v1/scrape-auth and on the trace tier's
// internal hop — while this doc called rotation a non-event.
//
// What no mechanism here can remove: the two ends read two different MOUNTS of
// the same Secret, and each kubelet updates its own on its own schedule. While
// the receiver's projection still carries the old value there is nothing to
// read, and a client whose projection already carries the new one is rejected
// until the receiver's kubelet catches up. Rotation is a non-event within one
// process's reach; the residual window is the projection skew, and it converges
// within DefaultRefreshInterval of the receiver's file actually changing.
//
// What is deliberately NOT unified is the fatality of the INITIAL read. A
// receiver that authenticates callers (the metadata service's /v1/scrape-auth,
// the trace tier's internal listener) must refuse to start without a token —
// an empty accept set means either an open endpoint or one that refuses
// everything, and neither is a state to discover in production. A CLIENT that
// presents a token (the kubelet scraper, the OTLP exporter) fails only the
// request it could not authenticate. NewRotating therefore reads fatally;
// NewFile does not read at all, and File.Read exists for the callers that want
// the fatal first read anyway.
//
// Not in scope: credential sources with a different lifecycle. The Azure
// Event Hubs reader's connection string and Entra token exchange
// (internal/agent/azurediag/auth.go) refresh against an expiry the issuer
// stamps, not against a file mtime, and share nothing with this but the word
// "token".
package bearer

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultReadInterval bounds how often a CLIENT re-reads the token it
	// presents, and how often a receiver with no traffic at all re-reads
	// (Rotating.Run's ticker). Long enough that a hot path (every kubelet
	// scrape, every OTLP export) does not touch the filesystem; short enough
	// that a rotation converges well inside the grace window below.
	DefaultReadInterval = time.Minute

	// DefaultRefreshInterval bounds how often a RECEIVER re-reads its accept
	// set. It is a second rather than a minute because the receiver's staleness
	// is the one direction of a rotation NOTHING else covers: a client that
	// re-read first presents a token the receiver has not seen, and there is no
	// grace for a value the receiver does not hold — it is a hard 401 for as
	// long as the receiver's copy lags. At a minute that was up to sixty
	// seconds of rejections per rotation; a second is one failed request that
	// the caller's own retry absorbs.
	//
	// The cost is bounded by the cadence, not by the request rate: an idle
	// receiver reads nothing (Tokens is only called by a request or by Run's
	// ticker) and a saturated one reads once per second. A FAILED read backs
	// off to DefaultReadInterval instead — a broken projection is not a
	// rotating one, and retrying it per second would turn one bad mount into a
	// per-second warn on every receiver in the fleet.
	DefaultRefreshInterval = time.Second

	// DefaultGrace keeps the PREVIOUS token accepted after a rotation, which is
	// what covers a client that has NOT re-read yet (they re-read on
	// DefaultReadInterval, independently of the receiver). The opposite
	// direction — a client that re-read FIRST — is covered by
	// DefaultRefreshInterval above, not by any grace: a receiver can only
	// accept tokens it has read.
	DefaultGrace = 5 * time.Minute
)

// ErrNoPath reports an empty token-file path. Callers name their own flag in
// the wrapping message — the same error means different things to the metadata
// service and to the trace tier.
var ErrNoPath = errors.New("no token file configured")

// Option tunes a File or a Rotating. The defaults are what every production
// caller wants; the options exist mostly so tests can drive the clock instead
// of sleeping.
type Option func(*settings)

type settings struct {
	interval time.Duration
	refresh  time.Duration
	grace    time.Duration
	now      func() time.Time
}

func resolve(opts []Option) settings {
	s := settings{interval: DefaultReadInterval, refresh: DefaultRefreshInterval, grace: DefaultGrace, now: time.Now}
	for _, o := range opts {
		o(&s)
	}
	// The receiver's cadence is never SLOWER than the periodic one: a caller
	// that shortens the interval is asking to notice a rotation sooner, and a
	// refresh above it would silently overrule that (and make Run's ticker
	// re-read nothing, since Tokens would refuse every tick).
	if s.refresh > s.interval {
		s.refresh = s.interval
	}
	return s
}

// WithInterval overrides the periodic re-read interval — a client's cadence,
// and a receiver's Run ticker. It also LOWERS a receiver's refresh cadence to
// match when it is the shorter of the two (see resolve): a caller asking to
// notice a rotation sooner must not be overruled by the refresh default.
func WithInterval(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.interval = d
		}
	}
}

// WithGrace overrides how long a rotated-away token stays accepted (Rotating
// only).
func WithGrace(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.grace = d
		}
	}
}

// WithClock injects the clock. Tests use it to step past the read interval and
// the grace window without sleeping.
func WithClock(now func() time.Time) Option {
	return func(s *settings) {
		if now != nil {
			s.now = now
		}
	}
}

// ReadFile reads and trims a token file. Trimming matters: a Secret mounted
// from `echo` or a here-doc carries a trailing newline, and every client sends
// the trimmed value in the header. An empty (or whitespace-only) file is an
// error rather than an empty token — an empty credential authenticates nothing
// and would turn a swap into a silent outage.
func ReadFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("token file %s is empty", path)
	}
	return tok, nil
}

// File is the CLIENT half: the token this process PRESENTS, re-read at most
// once per interval, with the last good value kept across a failed re-read.
//
// The zero value is not usable; call NewFile.
type File struct {
	path string
	log  *slog.Logger
	set  settings

	mu      sync.Mutex
	token   string
	fetched time.Time
	loaded  bool
}

// NewFile returns a token file reader. It does NOT read: a client's failure to
// authenticate is the request's failure, not the process's. Callers that want
// the read to be fatal at startup call Read explicitly.
func NewFile(path string, log *slog.Logger, opts ...Option) *File {
	if log == nil {
		log = slog.Default()
	}
	return &File{path: path, log: log, set: resolve(opts)}
}

// Path is the file being read (for log lines and error messages).
func (f *File) Path() string { return f.path }

// Read reads the file NOW, bypassing the interval, and caches the result. A
// caller wanting a fatal initial read calls this before anything starts.
func (f *File) Read() (string, error) {
	tok, err := ReadFile(f.path)
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	f.token, f.fetched, f.loaded = tok, f.set.now(), true
	f.mu.Unlock()
	return tok, nil
}

// Token returns the current token, re-reading the file when the cached value
// is stale.
//
// A failed re-read with a last good value returns THAT value and warns: a
// transient error during a Secret swap must not fail an export or a scrape that
// would otherwise have succeeded. A failure with no last good value is an
// error — there is nothing to present.
func (f *File) Token() (string, error) {
	f.mu.Lock()
	tok, loaded := f.token, f.loaded
	fresh := loaded && f.set.now().Sub(f.fetched) < f.set.interval
	f.mu.Unlock()
	if fresh {
		return tok, nil
	}
	next, err := f.Read()
	if err != nil {
		if !loaded {
			return "", err
		}
		f.log.Warn("re-reading token file; keeping the last good token", "path", f.path, "error", err)
		// Do not retry on every call while the file is broken: the last good
		// token is being served, and hammering the filesystem per request is
		// what the interval exists to prevent.
		f.mu.Lock()
		f.fetched = f.set.now()
		f.mu.Unlock()
		return tok, nil
	}
	return next, nil
}

// Get is Token with the error dropped, for callers that must produce a string
// (a token provider handed to another package). "" means nothing has ever been
// read successfully, and the request it feeds will fail unauthenticated —
// which is the honest outcome.
func (f *File) Get() string {
	tok, _ := f.Token()
	return tok
}

// Rotating is the SERVER half: the set of tokens a receiver ACCEPTS — the
// current file contents plus, for the grace window after a change, its
// predecessor.
//
// It re-reads on DefaultRefreshInterval rather than DefaultReadInterval,
// because the two ends of a rotation go out of step in both directions and only
// one of them has a grace window; see the package doc.
type Rotating struct {
	path string
	log  *slog.Logger
	set  settings

	mu        sync.Mutex
	cur, prev string
	prevUntil time.Time
	// nextRead is the earliest time the file may be read again: refresh after a
	// good read, the longer interval after a failed one.
	nextRead time.Time
}

// NewRotating reads the token file once, FATALLY: "no path", "unreadable" and
// "empty" all fail here rather than leaving a listener reachable from every pod
// in the cluster with an empty accept set. (An empty candidate can never
// authorize — see Authorized — so the failure would be a receiver that refuses
// everything, only marginally better than one that accepts everything.)
func NewRotating(path string, log *slog.Logger, opts ...Option) (*Rotating, error) {
	if strings.TrimSpace(path) == "" {
		return nil, ErrNoPath
	}
	if log == nil {
		log = slog.Default()
	}
	r := &Rotating{path: path, log: log, set: resolve(opts)}
	tok, err := ReadFile(path)
	if err != nil {
		return nil, err
	}
	r.cur, r.nextRead = tok, r.set.now().Add(r.set.refresh)
	return r, nil
}

// Run drives the re-read from a CLOCK until ctx is done. Every receiver that
// holds a Rotating should run it.
//
// It lives here rather than at the call sites because it is not optional and it
// is invisible when missing. Tokens() re-reads only when CALLED, and the grace
// window is armed at that moment — so on a listener with no traffic, a rotation
// is noticed by the first request AFTER it, which anchors the revoked token's
// grace window at that request instead of within one interval of the file
// change and stretches its acceptance far past the documented window. A
// receiver that simply never got the ticker therefore keeps accepting a revoked
// token indefinitely, with nothing to show for it.
//
// That is not hypothetical: it was written as a goroutine at one call site and
// forgotten at another, and the forgotten one was the authenticated internal
// hop of the trace tier. Making it a method of the type that owns the rotation
// contract is what stops the next receiver from having to remember.
func (r *Rotating) Run(ctx context.Context) {
	ticker := time.NewTicker(r.set.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Tokens()
		}
	}
}

// Tokens returns the accepted set, re-reading the file when stale. A failed or
// empty re-read keeps the last good value: a transient error during a Secret
// swap must not 401 the whole fleet.
//
// STALE here means DefaultRefreshInterval, not DefaultReadInterval. A client
// that re-read before the receiver presents a token no grace window can cover,
// so the receiver's own lag is the whole cost of that direction of a rotation
// and it is paid as a hard 401. A failed read backs off to the longer interval,
// because a file that cannot be read is not a file that is rotating.
//
// Run drives this from a ticker so a quiet listener still notices a rotation
// within one interval; request traffic refreshes it far sooner.
func (r *Rotating) Tokens() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.set.now()
	if !now.Before(r.nextRead) {
		// Do the file I/O with the lock DROPPED. This is every authenticated
		// request's read path (the metadata service's /v1/scrape-auth, the
		// trace tier's internal hop), so holding the mutex across os.ReadFile
		// would serialise every auth check behind a slow or wedged Secret mount
		// (a stuck CSI/NFS projection), not just the one request that triggered
		// the refresh. Claim the refresh slot first (advance nextRead) so a
		// concurrent caller sees the read as not-yet-due and returns the
		// last-good set instead of also reading; publish the result after.
		r.nextRead = now.Add(r.set.refresh)
		path := r.path
		r.mu.Unlock()
		next, err := ReadFile(path)
		r.mu.Lock()
		now = r.set.now()
		if err != nil {
			r.nextRead = now.Add(r.set.interval)
			r.log.Warn("re-reading token file; keeping the last good token", "path", path, "error", err)
		} else if next != r.cur {
			r.prev, r.prevUntil = r.cur, now.Add(r.set.grace)
			r.cur = next
			r.log.Info("bearer token rotated; the previous token stays accepted for the grace window",
				"path", path, "grace", r.set.grace)
		}
	}
	if r.prev != "" && now.Before(r.prevUntil) {
		return []string{r.cur, r.prev}
	}
	return []string{r.cur}
}

// Authorized reports whether an `Authorization: Bearer <token>` header carries
// one of the accepted tokens.
//
// Constant time (crypto/subtle), and EVERY candidate is compared with no early
// exit: a byte-at-a-time compare leaks the shared token to anyone who can time
// responses, and an early exit would leak which of a rotation pair matched.
// ConstantTimeCompare also returns 0 for differing lengths, which leaks only
// the token's length — unavoidable without hashing and not worth the machinery.
//
// An empty candidate never authorizes: an unconfigured receiver must reject,
// not accept an empty header.
func Authorized(header string, tokens []string) bool {
	got, ok := Parse(header)
	if !ok {
		return false
	}
	matched := 0
	for _, tok := range tokens {
		if tok == "" {
			continue
		}
		matched |= subtle.ConstantTimeCompare([]byte(got), []byte(tok))
	}
	return matched == 1
}

// Parse extracts the credentials of an `Authorization: Bearer <token>` header.
// The scheme is matched case-insensitively (RFC 9110 §11.1); the token is
// returned verbatim.
func Parse(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}
