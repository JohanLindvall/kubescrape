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
// Event Hubs reader's Entra token exchange (internal/agent/azurediag/auth.go)
// refreshes against an expiry the issuer stamps, not against a file mtime, and
// shares nothing with this but the word "token". Its connection string IS a
// mounted file, but it is re-read once per SASL session with no cache
// (connectionStringMechanism): there is no last-good value to keep and no read
// cadence to share, and a failed read fails that one session, which kgo
// retries.
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

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// reWarnInterval is how often a PERSISTING read failure re-warns. The first
// failure of a RUN always warns immediately, the repeats drop to Debug, and
// this is the cadence at which the condition is restated so a broken mount is
// not a single line lost in an hour of scrollback. Every failed read is
// counted regardless (kubescrape_bearer_token_read_errors_total), so the RATE
// never depends on the throttle.
//
// "Of a run" is the failing -> recovered -> failing TRANSITION, and it needs a
// bit of state beyond a throttle: logdedupe.Throttle's zero value fires once
// per PROCESS, so a second outage starting within reWarnInterval of the first
// one's last line would have opened at Debug — with the recovery Info line
// above it, that reads as a mount that got better and stayed better. Each half
// keeps a logdedupe.Outage, which holds that run state beside the throttle: it
// says whether a failure opens a run, bounds the repeats, and makes the
// opening line CLAIM the throttle slot so the two never emit back to back.
const reWarnInterval = 5 * time.Minute

// Roles reported on kubescrape_bearer_token_read_errors_total. The two halves
// fail differently and an operator has to tell them apart: a client keeps
// presenting its last good token (or none at all, and every request it feeds
// fails), while a receiver keeps ACCEPTING its last good set — so a rotation
// the receiver cannot read is a fleet-wide 401 the moment the clients catch
// up.
const (
	roleClient   = "client"
	roleReceiver = "receiver"
)

const (
	// DefaultReadInterval bounds how often a CLIENT re-reads the token it
	// presents, and how long a RECEIVER backs off once a read failure PERSISTS
	// (a second failure in a row; the first is retried at
	// DefaultRefreshInterval). Long enough that a hot path (every kubelet
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
	// The cost is bounded by the cadence, not by the request rate: a receiver
	// reads once per second whether it is idle (Run's ticker runs at this
	// cadence) or saturated (Tokens refuses to re-read sooner). A read failure
	// that PERSISTS backs off to DefaultReadInterval instead — a broken
	// projection is not a rotating one, and re-reading it per second would be a
	// per-second failed read (and a counter increment and a Debug line; the
	// warn itself is throttled, see reWarnInterval) on every receiver in the
	// fleet, about a state that is not changing. The FIRST failure of a run is
	// retried at this cadence once more, since a swap can produce one transient
	// failure and backing off from it would blind the receiver to the new token
	// for the whole minute.
	DefaultRefreshInterval = time.Second

	// DefaultGrace keeps the PREVIOUS token accepted after a rotation, which is
	// what covers a client that has NOT re-read yet (they re-read on
	// DefaultReadInterval, independently of the receiver). The opposite
	// direction — a client that re-read FIRST — is covered by
	// DefaultRefreshInterval above, not by any grace: a receiver can only
	// accept tokens it has read.
	DefaultGrace = 5 * time.Minute
)

// refreshWait bounds how long the caller that CLAIMS a due re-read waits for
// it. The read itself runs on its own goroutine (await), which publishes and
// reports its outcome whether or not anyone is still waiting.
//
// On a working mount this changes nothing: a token file reads in microseconds,
// so the claimer still gets the answer its own call triggered — which is what
// lets a receiver accept a token a client rotated to first
// (TestRotatingAcceptsATokenTheClientRotatedToFirst), and what lets a client
// present a rotated token on the first call past its interval. Answering every
// claimer from the old value instead would turn both into a 401 and a retry.
//
// On a WEDGED mount (a stuck CSI/NFS projection, where open(2) never returns)
// it is the whole cost, paid ONCE per wedge: the claimer returns the last good
// value after this long, every later caller answers from it immediately because
// the claim stays held until the read returns (flight), and exactly one
// goroutine stays parked in the read. The one exception is a File that has
// NEVER read a token: there is no last good value to answer from, so each
// caller waits for the in-flight read up to this bound (File.awaitFirstRead)
// and then fails its request — bounded per call, and never in the syscall. Without the bound the claimer was parked
// with it, and who the claimer IS is what made that matter: on the trace tier's
// internal hop it is grpc-go's InTapHandle, which runs on the connection's
// frame-read loop with the transport's mutex held, so one wedged read froze a
// sibling shard's whole connection until the mount recovered; on the client
// half it is an OTLP export or a kubelet scrape — the tailer's single sweep
// goroutine among them — which blocked in open(2) while holding a perfectly
// good cached token.
const refreshWait = 100 * time.Millisecond

// flight is the single-flight claim on a re-read that runs with its owner's
// mutex dropped, shared by both halves and guarded by that mutex.
//
// The claim is a FLAG, never a deadline, and the difference is the whole
// point: a timestamp claim EXPIRES while a read is still blocked, so on a mount
// where open(2) never returns every interval would start another blocking read.
// Each parks a goroutine in a file syscall, which pins an OS thread (regular
// files are not pollable), and the process walks into the runtime's fatal
// 10000-thread limit in hours. The flag bounds the reads in flight at ONE for
// as long as the wedge lasts, while every other caller answers from the last
// good value.
type flight struct {
	// reading is true from a claim until its read returns.
	reading bool
	// gen numbers the claims, so a claimer that stopped waiting can tell ITS
	// read from a later one before reporting a stall.
	gen uint64
	// stalled is true when the in-flight read outlived refreshWait and that was
	// reported; loud says it was reported at Warn, so its completion is
	// reported at the matching level.
	stalled, loud bool
	// warn throttles the stall report: a mount that is slow rather than wedged
	// (every read a little over refreshWait) is a persisting condition, and one
	// line per read would be one per second per receiver.
	warn logdedupe.Throttle
	// done is closed when the in-flight read returns. It is made LAZILY, by
	// the first caller that has to wait for the read (returned), so a claim
	// nobody waits on — every re-read of a loaded token — allocates nothing
	// for it.
	done chan struct{}
}

// claim takes the read slot, or reports that another caller holds it.
func (c *flight) claim() (gen uint64, ok bool) {
	if c.reading {
		return 0, false
	}
	c.reading = true
	c.gen++
	return c.gen, true
}

// returned is closed when the in-flight read returns. Call it only while a
// read is in flight (reading), under the owner's mutex.
func (c *flight) returned() <-chan struct{} {
	if c.done == nil {
		c.done = make(chan struct{})
	}
	return c.done
}

// release ends the claim when its read returns, reporting whether that read
// had been reported as stalled (and at which level), and wakes every caller
// waiting on returned.
func (c *flight) release() (stalled, loud bool) {
	stalled, loud = c.stalled, c.loud
	c.reading, c.stalled, c.loud = false, false, false
	if c.done != nil {
		close(c.done)
		c.done = nil
	}
	return stalled, loud
}

// stall records that the claimer holding gen stopped waiting. report is false
// when that read already returned — or a later claim replaced it — in which
// case there is nothing to say.
func (c *flight) stall(gen uint64) (report, loud bool) {
	if !c.reading || c.gen != gen || c.stalled {
		return false, false
	}
	c.stalled, c.loud = true, c.warn.Allow(reWarnInterval)
	return true, c.loud
}

// await runs read on its own goroutine and waits at most wait for it,
// reporting whether it finished. The read owns its outcome end to end —
// publishing under the owner's mutex, counting, logging — so a caller that
// stops waiting loses only the freshness of its own answer, never the read's
// result. The goroutine, the channel and the timer are paid once per re-read,
// never on the steady-state path.
func await(wait time.Duration, read func()) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		read()
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// logStall reports a claimed read that outlived refreshWait, and logResumed its
// eventual return. Both run OFF the claimer's goroutine: the claimer may be an
// auth check holding a transport mutex (see refreshWait), and a log record is a
// blocking write to stderr — on a node whose Secret mount has wedged, the log
// pipe is not a safe bet either.
func logStall(log *slog.Logger, loud bool, msg, path string, wait time.Duration) {
	level := slog.LevelDebug
	if loud {
		level = slog.LevelWarn
	}
	log.Log(context.Background(), level, msg, "tokenFile", path, "wait", wait)
}

func logResumed(log *slog.Logger, loud bool, path string) {
	level := slog.LevelDebug
	if loud {
		level = slog.LevelInfo
	}
	log.Log(context.Background(), level, "the stalled bearer token file read returned; re-reading resumes", "tokenFile", path)
}

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
	// wait bounds how long a caller that claims a due re-read waits for it
	// (refreshWait). Not an Option: nothing in production has a reason to move
	// it, and a test that needs another value sets it on the struct.
	wait time.Duration
	now  func() time.Time
}

func resolve(opts []Option) settings {
	s := settings{interval: DefaultReadInterval, refresh: DefaultRefreshInterval, grace: DefaultGrace, wait: refreshWait, now: time.Now}
	for _, o := range opts {
		o(&s)
	}
	// The receiver's cadence is never SLOWER than the periodic one: a caller
	// that shortens the interval is asking to notice a rotation sooner, and a
	// refresh above it would silently overrule that (Run's ticker runs at the
	// refresh cadence, so it would not tick any faster either).
	if s.refresh > s.interval {
		s.refresh = s.interval
	}
	return s
}

// WithInterval overrides the periodic re-read interval — a client's cadence,
// and a receiver's back-off once a read failure persists. It also LOWERS a receiver's
// refresh cadence (and with it Run's ticker) to match when it is the shorter of
// the two (see resolve): a caller asking to notice a rotation sooner must not
// be overruled by the refresh default.
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

// readFile reads and trims a token file. Trimming matters: a Secret mounted
// from `echo` or a here-doc carries a trailing newline, and every client sends
// the trimmed value in the header. An empty (or whitespace-only) file is an
// error rather than an empty token — an empty credential authenticates nothing
// and would turn a swap into a silent outage.
func readFile(path string) (string, error) {
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
	// firstErr is the error of the most recent failed read while nothing has
	// ever loaded. Only a caller that WAITED for that read consults it
	// (awaitFirstRead), so it can report what the read found rather than that
	// the read "has not returned" — which, by then, it has.
	firstErr error
	// outage is the run of failed re-reads, open while the LAST re-read
	// failed. Its first failure warns at once, the repeats re-warn at
	// reWarnInterval and drop to Debug between, and it is what lets the
	// recovery be reported: a warn with no matching "it works again" leaves an
	// operator unable to tell a fixed mount from one nobody is looking at any
	// more (the transition shape cmd/kubescrape/apiserver.go established).
	outage logdedupe.Outage
	// flight bounds the re-reads in flight at ONE. Every caller of a client
	// token is on a hot path — every OTLP export, every kubelet scrape, every
	// /v1/scrape-auth fetch — and without the claim each caller that found the
	// cache stale ran its own read: on a wedged mount all of them parked in
	// open(2), one pinned OS thread each, while the last good token sat unused
	// in f.token. Rotating had the claim; this half never got it.
	flight flight
}

// errReadPending is the answer when there is no token to present AND the read
// that could produce one has not returned within refreshWait. A caller waits
// for that read at most that long, on a channel rather than in the syscall, so
// a wedged mount costs it the same bounded wait it costs the claimer — and past
// it there is nothing to hand out.
var errReadPending = errors.New("the token file read has not returned yet, and no token has ever been read")

// NewFile returns a token file reader. It does NOT read: a client's failure to
// authenticate is the request's failure, not the process's. Callers that want
// the read to be fatal at startup call Read explicitly.
func NewFile(path string, log *slog.Logger, opts ...Option) *File {
	if log == nil {
		log = slog.Default()
	}
	return &File{path: path, log: log, set: resolve(opts)}
}

// Read reads the file NOW, bypassing the interval, and caches the result. A
// caller wanting a fatal initial read calls this before anything starts.
func (f *File) Read() (string, error) {
	tok, err := readFile(f.path)
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	changed, recovered := f.storeLocked(tok)
	f.mu.Unlock()
	f.reportStored(tok, changed, recovered)
	return tok, nil
}

// storeLocked publishes a successfully read token. f.mu must be held.
func (f *File) storeLocked(tok string) (changed, recovered bool) {
	now := f.set.now()
	changed = f.loaded && tok != f.token
	_, _, recovered = f.outage.Recover(now)
	f.token, f.fetched, f.loaded, f.firstErr = tok, now, true, nil
	return changed, recovered
}

// reportStored reports what storeLocked decided, with the lock released.
//
// A CHANGED value is one Info line, because it is the event every 401 burst has
// to be correlated against: the two ends of a rotation re-read on their own
// cadences, so "which side moved first, and when" is the whole question and
// neither side could previously answer it. The token itself is never logged —
// its LENGTH is, which is enough to tell a real credential from a truncated
// projection and discloses nothing a timing-safe compare does not already leak
// (see Authorized).
func (f *File) reportStored(tok string, changed, recovered bool) {
	if changed {
		f.log.Info("bearer token file changed; presenting the new token", "tokenFile", f.path, "bytes", len(tok))
	}
	if recovered {
		f.log.Info("re-reading the bearer token file succeeded again", "tokenFile", f.path)
	}
}

// Token returns the current token, re-reading the file when the cached value
// is stale.
//
// A failed re-read with a last good value returns THAT value and warns: a
// transient error during a Secret swap must not fail an export or a scrape that
// would otherwise have succeeded. A failure with no last good value is an
// error — there is nothing to present.
//
// At most ONE re-read is ever in flight (flight). The caller that claims it
// waits for it at most refreshWait — so a rotated token is presented on the
// first call past the interval — and every caller that finds a read already in
// flight answers from the last good token without touching the file. With
// NOTHING ever read there is no last good token, so such a caller waits for
// the in-flight read instead, just as boundedly (awaitFirstRead): concurrent
// FIRST uses — the kubelet scrapes a cycle spawns back to back — must all get
// the token a healthy mount produces in microseconds, not one token and a
// "read pending" error each for the rest. On a wedged mount that is one parked
// goroutine and bounded waits, not one parked caller per request.
func (f *File) Token() (string, error) {
	f.mu.Lock()
	tok, loaded := f.token, f.loaded
	if loaded && f.set.now().Sub(f.fetched) < f.set.interval {
		f.mu.Unlock()
		return tok, nil
	}
	gen, claimed := f.flight.claim()
	var pending <-chan struct{}
	if !claimed && !loaded {
		pending = f.flight.returned()
	}
	f.mu.Unlock()
	if pending != nil {
		return f.awaitFirstRead(pending)
	}
	if !claimed {
		return f.lastGood(tok, loaded)
	}
	var (
		next string
		err  error
	)
	// next and err are read only when await reports the read finished: the
	// channel close is what orders the goroutine's writes before this read,
	// and a read that did not finish may still write them later.
	if await(f.set.wait, func() { next, err = f.refresh() }) {
		return next, err
	}
	f.mu.Lock()
	report, loud := f.flight.stall(gen)
	tok, loaded = f.token, f.loaded
	f.mu.Unlock()
	if report {
		msg := "re-reading the bearer token file has not returned; presenting the last good token, and no further read starts until this one returns"
		if !loaded {
			msg = "reading the bearer token file has not returned and no token has ever been read; every request that needs it fails until a read succeeds"
		}
		go logStall(f.log, loud, msg, f.path, f.set.wait)
	}
	return f.lastGood(tok, loaded)
}

// lastGood is the answer while a re-read is in flight: the cached token, or —
// when nothing was ever read and the read did not return within refreshWait —
// an error, because there is nothing to hand out.
func (f *File) lastGood(tok string, loaded bool) (string, error) {
	if loaded {
		return tok, nil
	}
	return "", fmt.Errorf("bearer token file %s: %w", f.path, errReadPending)
}

// awaitFirstRead answers a caller that found the FIRST read already in flight.
// It waits for that read at most refreshWait — the claimer's bound, and on a
// channel rather than in the syscall, so a wedged mount still costs it no more
// than it costs the claimer — then answers with what the read found: the
// token, the read's own error, or errReadPending when it has not returned.
func (f *File) awaitFirstRead(returned <-chan struct{}) (string, error) {
	timer := time.NewTimer(f.set.wait)
	defer timer.Stop()
	select {
	case <-returned:
	case <-timer.C:
		return f.lastGood("", false)
	}
	f.mu.Lock()
	tok, loaded, err := f.token, f.loaded, f.firstErr
	f.mu.Unlock()
	if !loaded && err != nil {
		return "", err
	}
	return f.lastGood(tok, loaded)
}

// refresh is a claimed re-read, run by await on its own goroutine. It owns the
// whole outcome — publish, count, report — because the caller that claimed it
// may have stopped waiting.
func (f *File) refresh() (string, error) {
	tok, err := readFile(f.path)
	f.mu.Lock()
	stalled, loud := f.flight.release()
	if err == nil {
		changed, recovered := f.storeLocked(tok)
		f.mu.Unlock()
		if stalled {
			logResumed(f.log, loud, f.path)
		}
		f.reportStored(tok, changed, recovered)
		return tok, nil
	}
	last, loaded := f.token, f.loaded
	now := f.set.now()
	// Decided here, where the transition is visible: a failure that OPENS a run
	// always warns, however recently the previous run's last line was written
	// (see reWarnInterval), and the repeats warn at most once per interval.
	_, warn := f.outage.Fail(now, reWarnInterval)
	if loaded {
		// Do not retry on every call while the file is broken: the last good
		// token is being served, and hammering the filesystem per request is
		// what the interval exists to prevent.
		f.fetched = now
	} else {
		f.firstErr = err
	}
	f.mu.Unlock()
	if stalled {
		logResumed(f.log, loud, f.path)
	}
	obs.BearerTokenReadErrors.WithLabelValues(roleClient).Inc()
	if !loaded {
		// Nothing to present, so every request this token feeds fails: a Token
		// caller gets the error and fails the request locally, sending nothing,
		// and Get's "" is rejected by whatever it is sent to. The caller reports
		// that as ITS failure (an export, a scrape); this line is the one that
		// says the cause is a credential file, and names it as tokenFile.
		f.noteFailure("no bearer token has ever been read; every request that needs it fails until the file is readable", err, warn)
		return "", err
	}
	f.noteFailure("re-reading the bearer token file failed; keeping the last good token", err, warn)
	return last, nil
}

// noteFailure reports a failed read at Warn when f.outage decided it is loud
// (the first failure of a run, then once per reWarnInterval) and at Debug
// otherwise. The counter moves on every failure (in the caller), so the
// throttle bounds the LOG and never the rate.
func (f *File) noteFailure(msg string, err error, warn bool) {
	if warn {
		f.log.Warn(msg, "tokenFile", f.path, "error", err)
		return
	}
	f.log.Debug(msg, "tokenFile", f.path, "error", err)
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
	// good read or the first failure of a run, the longer interval once a
	// failure persists.
	nextRead time.Time
	// flight is the claim on a refresh read in flight with the mutex dropped.
	// It is what bounds the number of concurrent reads to ONE, and a deadline
	// cannot do that job (see flight). Holding the lock across the read pinned
	// exactly one thread and survived indefinitely; the claim keeps that bound
	// while still letting every other caller answer from the last-good set, and
	// refreshWait bounds what the CLAIMER itself pays.
	flight flight
	// outage is the run of failed reads, open while the last read failed: it
	// decides the first-of-run warn and the throttled re-warn, backs off the
	// next read once a failure persists, and makes the recovery one Info line
	// rather than a warn that simply stops.
	outage logdedupe.Outage
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
	tok, err := readFile(path)
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
//
// It ticks at the REFRESH cadence (DefaultRefreshInterval), not the read
// interval, because it is also what keeps Cached current: a caller that must
// not do I/O (the trace tier's gRPC auth tap) reads the set Run maintains, and
// a client that rotated FIRST is refused there until the next refresh — a
// second, not a minute. The cadence does not re-read a broken mount any faster:
// a persisting read failure still backs Tokens off to the read interval.
func (r *Rotating) Run(ctx context.Context) {
	ticker := time.NewTicker(r.set.refresh)
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
// and it is paid as a hard 401. A read failure that PERSISTS backs off to the
// longer interval, because a file that cannot be read is not a file that is
// rotating; the first failure of a run is retried at the refresh cadence, since
// a swap can produce one transient failure.
//
// Run drives this from a ticker so a quiet listener still notices a rotation
// within one refresh interval.
//
// It never waits on the file for longer than refreshWait. The caller that
// claims a due refresh waits for it that long at most — so it still accepts a
// token a client rotated to first — and a caller that finds a refresh already
// in flight answers from the last-good set at once; on a wedged mount the read
// itself stays parked on its own goroutine (see refreshWait and flight). A
// caller that must not wait even that long uses Cached instead.
func (r *Rotating) Tokens() []string {
	r.mu.Lock()
	now := r.set.now()
	if !now.Before(r.nextRead) {
		if gen, ok := r.flight.claim(); ok {
			// Do the file I/O with the lock DROPPED. This is every authenticated
			// request's read path (the metadata service's /v1/scrape-auth, the
			// trace tier's internal hop), so holding the mutex across
			// os.ReadFile would serialise every auth check behind a slow or
			// wedged Secret mount (a stuck CSI/NFS projection), not just the one
			// request that triggered the refresh. Concurrent callers return the
			// last-good set instead of also reading — gated by the claim, NOT by
			// the advanced deadline, which expires under exactly the wedged
			// mount this exists for (see flight).
			r.nextRead = now.Add(r.set.refresh)
			r.mu.Unlock()
			finished := await(r.set.wait, r.refresh)
			r.mu.Lock()
			if !finished {
				if report, loud := r.flight.stall(gen); report {
					go logStall(r.log, loud,
						"re-reading the bearer token file has not returned; still accepting the last good token, and no further read starts until this one returns",
						r.path, r.set.wait)
				}
			}
			now = r.set.now()
		}
	}
	if r.prev != "" && !now.Before(r.prevUntil) {
		// Past the grace the predecessor is a revoked credential, and a
		// revoked credential is not something to keep in memory for the
		// process lifetime — nor to hand out, however harmlessly.
		r.prev, r.prevUntil = "", time.Time{}
	}
	out := []string{r.cur}
	if r.prev != "" {
		out = append(out, r.prev)
	}
	r.mu.Unlock()
	return out
}

// refresh is a claimed re-read, run by await on its own goroutine: it reads,
// publishes under r.mu and reports, because the caller that claimed it may have
// stopped waiting.
func (r *Rotating) refresh() {
	path := r.path // immutable after construction
	next, err := readFile(path)
	// What the re-read DECIDED, to be reported once the lock is gone.
	var (
		warn      bool
		recovered bool
		rotated   bool
		newBytes  int
	)
	r.mu.Lock()
	stalled, loud := r.flight.release()
	now := r.set.now()
	if err != nil {
		// Back off only once the failure PERSISTS. The first failure of a run is
		// retried at the refresh cadence, because it may be the transient read a
		// Secret swap can produce (the package doc) — and a receiver blind to the
		// swapped-in token for a whole read interval 401s every client that
		// re-read first for that long, which is the lag DefaultRefreshInterval
		// exists to remove. A second failure in a row is a broken projection
		// rather than a rotating one, and that is what the longer interval is
		// for. The cost is one extra read per failure run.
		if r.outage.Failing() {
			r.nextRead = now.Add(r.set.interval)
		} else {
			r.nextRead = now.Add(r.set.refresh)
		}
		// Decided here, where the transition is visible: a failure that OPENS a
		// run always warns, however recently the previous run's last line was
		// written (see reWarnInterval), and the repeats warn at most once per
		// interval.
		_, warn = r.outage.Fail(now, reWarnInterval)
	} else {
		_, _, recovered = r.outage.Recover(now)
		if next != r.cur {
			r.prev, r.prevUntil = r.cur, now.Add(r.set.grace)
			r.cur = next
			rotated, newBytes = true, len(next)
		}
	}
	r.mu.Unlock()

	// Report with the lock RELEASED, for the same reason the read drops it. An
	// slog record is a handler call plus a write to the process's stderr, and
	// that write BLOCKS when nothing drains the other end (a stalled log
	// collector, a full log disk, a 64 KiB pipe with no reader): emitting it
	// under r.mu would park every concurrent auth check — every agent's
	// /v1/scrape-auth fetch, every sibling shard's internal span push — behind a
	// log line, which is exactly the serialisation the lock drop exists to
	// prevent. The failure branch fires precisely when the mount is already
	// unhealthy, so the stall would land at the receiver's worst moment. Decide
	// under the lock, emit after: the shape File uses too. (Running on await's
	// goroutine, a stalled write here also keeps the claimer waiting no longer
	// than refreshWait.)
	if stalled {
		logResumed(r.log, loud, path)
	}
	if err != nil {
		// Counted on EVERY failure; the line is throttled. A receiver that
		// cannot read its accept set keeps accepting the last good token, so
		// the visible symptom arrives late and elsewhere — a fleet-wide 401
		// once the clients have rotated past it — which is why the condition
		// has to be reported where it is known.
		obs.BearerTokenReadErrors.WithLabelValues(roleReceiver).Inc()
		if warn {
			r.log.Warn("re-reading the bearer token file failed; still accepting the last good token, so a rotation will 401 every caller", "tokenFile", path, "error", err)
		} else {
			r.log.Debug("re-reading the bearer token file failed; still accepting the last good token", "tokenFile", path, "error", err)
		}
	}
	if recovered {
		r.log.Info("re-reading the bearer token file succeeded again", "tokenFile", path)
	}
	if rotated {
		// bytes, never the token: a length tells a real credential from a
		// truncated or half-written projection, and is all the constant-time
		// compare in Authorized leaks anyway.
		r.log.Info("bearer token rotated; the previous token stays accepted for the grace window",
			"tokenFile", path, "grace", r.set.grace, "bytes", newBytes)
	}
}

// Cached returns the accepted set WITHOUT ever reading the file or logging —
// what Tokens would answer, minus its refresh. It applies the predecessor's
// grace expiry exactly as Tokens does, so a revoked token is not accepted here
// past its window.
//
// It exists for the one caller that must not block: a gRPC InTapHandle (the
// trace tier's internal hop) runs on the connection's frame-read goroutine with
// the transport's mutex held, so a refresh read claimed there — on a wedged
// CSI/NFS projection, one that never returns — froze every stream on that
// connection until the mount recovered or the connection aged out. Freshness
// comes from Run, which ticks at the refresh cadence for exactly this reason;
// a receiver that uses Cached MUST run it, or the set never moves.
func (r *Rotating) Cached() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.prev != "" && !r.set.now().Before(r.prevUntil) {
		r.prev, r.prevUntil = "", time.Time{}
	}
	out := []string{r.cur}
	if r.prev != "" {
		out = append(out, r.prev)
	}
	return out
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
	got, ok := parseHeader(header)
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

// parseHeader extracts the credentials of an `Authorization: Bearer <token>`
// header. The scheme is matched case-insensitively (RFC 9110 §11.1);
// surrounding whitespace is trimmed from the token, which is otherwise returned
// byte for byte (not decoded), and an all-whitespace token is not a credential.
func parseHeader(header string) (string, bool) {
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
