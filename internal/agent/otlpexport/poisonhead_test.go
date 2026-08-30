package otlpexport

// One payload the collector will never accept must not hold the QUEUE HEAD —
// and the fix for that must not cost a GOOD payload its retry depth.
//
// The disk buffer is a node-shared resource and its commits are a cursor:
// nothing behind the head can retire while the head is being retried. Before
// this, a stuck head spent a whole five-attempt cycle at the head — with the
// backoff persisting across cycles and capped at 30s, up to ~2 minutes — before
// rotating to the back, and the accountable-failure evidence the drop rule
// needs accrued at most once per rotation. Four rotations to a drop meant one
// unauthenticated sender pushing records no collector will accept (a single
// over-4-MiB log record does it) stalled the logs of every other pod on that
// node for minutes at a time, repeatedly.
//
// The short lap that fixes it is gated on the PAYLOAD's own poison evidence
// (sink.accountableLap), never on the sink merely being warm: respondedError is
// true for the classes IsPermanent deliberately treats as transient — a
// rotating bearer token's 401, a collector rollout's 404 — so a warm-sink gate
// cut every good batch's cycle to one attempt during exactly the two events the
// depth exists for. TestGoodPayloadKeepsItsFullRetryDepthOnAWarmSink is that
// half, and it is the one that fails loudest if the gate ever drifts back.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// poisonSender accepts everything except log batches whose body starts with
// "poison", which it refuses the way a collector refuses an over-limit message:
// ResourceExhausted, which OTLP classifies retryable and IsPermanent therefore
// does not catch — the exact class that used to circle the queue forever.
type poisonSender struct {
	mu       sync.Mutex
	poison   int // attempts spent on the poison payload
	good     int // batches accepted
	bodies   []string
	refuseAl bool // refuse EVERYTHING (the outage case)
	// onAccept is the backlog keeper's replacement handshake; see backlogKeeper.
	onAccept func()
}

func (p *poisonSender) ExportLogs(_ context.Context, ld plog.Logs) error {
	p.mu.Lock()
	body := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
	if p.refuseAl || strings.HasPrefix(body, "poison") {
		p.poison++
		p.mu.Unlock()
		return status.Error(codes.ResourceExhausted, "message too large")
	}
	p.good++
	p.bodies = append(p.bodies, body)
	onAccept := p.onAccept
	p.mu.Unlock()
	acceptDelay()
	if onAccept != nil {
		onAccept()
	}
	return nil
}

func (p *poisonSender) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

func (p *poisonSender) counts() (poison, good int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.poison, p.good
}

// acceptDelay is what a delivery to a real collector costs. keepBacklogDeep no
// longer depends on it to keep the queue non-empty — the depth is conserved by
// handshake now, not by outrunning the drain — but a sender that returns
// instantly still makes these tests spin the whole drain loop at wire speed for
// nothing, so the pacing stays.
func acceptDelay() { time.Sleep(200 * time.Microsecond) }

// keepBacklogDeep keeps `depth` records queued behind the head for the duration
// of the test. Both integration tests below depend on the shape a real node has
// and a quiet test does not: OTHER batches waiting behind the head, so a
// rotation actually rotates and deliveries land BETWEEN the head's laps. That is
// what makes a lap accountable at all — with an empty queue behind it, Requeue
// reports nothing to rotate past, the head stays put, and the next lap correctly
// keeps its full backed-off cycle.
//
// It maintains the depth by CONSERVATION, not by racing the drain, and that is
// the whole point of the handshake below. The first version was a background
// goroutine topping the queue up whenever `stats().Backlog` dipped: on a quiet
// machine it kept up, and under a loaded `go test -race ./...` it did not — the
// drain emptied the queue behind the head, one lap in maybe five stopped being
// accountable, and that lap cost stuckAfterAttempts instead of 1. The assertion
// below then read 13 wire attempts against a want of 8 (5+5+1+1+1: one extra
// full cycle), i.e. a real scheduler race presenting as a broken bound. So the
// sender now ASKS for a replacement for every good batch it accepts and waits
// for that record to be queued before the drain moves on: the depth is an
// invariant of the loop rather than the outcome of a race, and the attempt count
// is exactly what the design predicts on any machine.
//
// The producer stays on its own goroutine because that is where a real one is —
// enqueueing into the same diskqueue the drain holds a reservation on is the
// production shape, and doing it inline on the drain goroutine would be a
// re-entrant queue call this code never makes.
//
// It returns the sender's replace hook (install it as the sender's onAccept)
// and the keeper's stop func.
func keepBacklogDeep(t *testing.T, b *Buffered, buf *Buffer, body string, depth int64) (replace func(), stop func()) {
	t.Helper()
	req := make(chan chan struct{})
	quit := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once

	add := func() {
		if err := b.ExportLogs(context.Background(), logsWith(body)); err != nil {
			t.Errorf("keepBacklogDeep enqueue: %v", err)
		}
	}
	// Pre-fill synchronously, so the drain never starts against a queue the
	// producer has not reached yet.
	for buf.stats().Backlog < depth {
		add()
	}
	go func() {
		defer close(done)
		for {
			select {
			case <-quit:
				return
			case ack := <-req:
				add()
				close(ack)
			}
		}
	}()
	// replace is what the sender calls after accepting a batch. It returns
	// immediately once the keeper is stopped, so a drain still in flight during
	// teardown cannot block on a goroutine that has gone away.
	replace = func() {
		ack := make(chan struct{})
		select {
		case req <- ack:
			select {
			case <-ack:
			case <-quit:
			}
		case <-quit:
		}
	}
	// Idempotent: every caller both defers it and calls it before inspecting the
	// counters, and a second close(stop) would panic.
	return replace, func() { once.Do(func() { close(quit) }); <-done }
}

// The whole finding, end to end: a poison head must be given up on within a few
// laps and must not spend a full retry cycle at the head on each of them.
func TestPoisonHeadDoesNotHoldTheQueueForEveryOtherProducer(t *testing.T) {
	send := &poisonSender{}
	dir := t.TempDir()
	ls, err := OpenBuffer(dir+"/logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	b := NewBuffered(send, ls, nil, nil, 20*time.Millisecond, quietLogger())

	// The head is the payload nothing will accept.
	if err := b.ExportLogs(context.Background(), logsWith("poison-1")); err != nil {
		t.Fatal(err)
	}
	before := obs.BufferDroppedBatches.WithLabelValues("logs").Value()

	// Everything else on the node keeps logging behind it.
	replace, stopProducing := keepBacklogDeep(t, b, ls, "good", 8)
	send.mu.Lock()
	send.onAccept = replace
	send.mu.Unlock()
	defer stopProducing()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	drained := make(chan struct{})
	go func() { b.Run(ctx); close(drained) }()

	deadline := time.Now().Add(20 * time.Second)
	for obs.BufferDroppedBatches.WithLabelValues("logs").Value()-before < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stopProducing()
	cancel()
	<-drained

	if got := obs.BufferDroppedBatches.WithLabelValues("logs").Value() - before; got != 1 {
		t.Fatalf("dropped batches = %v, want the one poison payload", got)
	}
	poison, good := send.counts()
	// One full cycle (the first sighting, before any evidence existed about this
	// payload) plus one attempt for each accountable lap the drop rule needs.
	if want := stuckAfterAttempts + maxDrainCycles; poison > want {
		t.Fatalf("the poison payload cost %d wire attempts before it was given up on, want at most %d: "+
			"every one of them is spent HOLDING THE QUEUE HEAD, so a batch nothing will accept stalls "+
			"every other producer on the node", poison, want)
	}
	if good == 0 {
		t.Fatal("no other batch got through at all: the test proves nothing")
	}
}

// authFlakySender fails the ONE named body with a RESPONDED transient error —
// the 401 a bearer-token rotation produces, which IsPermanent deliberately
// classifies transient — for `fails` attempts and accepts it afterwards.
// Everything else is accepted, which is what keeps the sink demonstrably warm.
type authFlakySender struct {
	mu        sync.Mutex
	body      string
	fails     int
	tries     int
	delivered bool
	// onAccept is the backlog keeper's replacement handshake; see backlogKeeper.
	onAccept func()
}

func (a *authFlakySender) ExportLogs(_ context.Context, ld plog.Logs) error {
	a.mu.Lock()
	if ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str() != a.body {
		onAccept := a.onAccept
		a.mu.Unlock()
		acceptDelay()
		if onAccept != nil {
			onAccept()
		}
		return nil
	}
	a.tries++
	if a.fails > 0 {
		a.fails--
		a.mu.Unlock()
		return status.Error(codes.Unauthenticated, "token rotating")
	}
	a.delivered = true
	a.mu.Unlock()
	return nil
}

func (a *authFlakySender) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

func (a *authFlakySender) state() (tries int, delivered bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.tries, a.delivered
}

// The regression the short lap must not reintroduce: a GOOD payload meeting a
// responded transient error for four consecutive attempts is still delivered on
// the fifth, however warm the sink is and however deep the backlog behind it.
//
// With the gate on "the collector delivered something since the last failure
// here" this batch got ONE attempt per lap, and — because the deliveries behind
// it kept landing between its laps — spent the whole poison budget and was
// DROPPED on its fourth lap, four attempts in. That is a Secret rotation or a
// collector rollout destroying data the buffer exists to carry through them.
func TestGoodPayloadKeepsItsFullRetryDepthOnAWarmSink(t *testing.T) {
	send := &authFlakySender{body: "rotating-token", fails: stuckAfterAttempts - 1}
	dir := t.TempDir()
	ls, err := OpenBuffer(dir+"/logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	b := NewBuffered(send, ls, nil, nil, 2*time.Millisecond, quietLogger())

	// Other producers deliver ahead of it and keep delivering behind it, so the
	// sink is warm at every point where the old gate looked.
	for range 5 {
		if err := b.ExportLogs(context.Background(), logsWith("good")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.ExportLogs(context.Background(), logsWith(send.body)); err != nil {
		t.Fatal(err)
	}
	before := obs.BufferDroppedBatches.WithLabelValues("logs").Value()
	replace, stopProducing := keepBacklogDeep(t, b, ls, "good", 8)
	send.mu.Lock()
	send.onAccept = replace
	send.mu.Unlock()
	defer stopProducing()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	drained := make(chan struct{})
	go func() { b.Run(ctx); close(drained) }()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := send.state(); ok {
			break
		}
		if obs.BufferDroppedBatches.WithLabelValues("logs").Value()-before > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	stopProducing()
	cancel()
	<-drained

	tries, delivered := send.state()
	if got := obs.BufferDroppedBatches.WithLabelValues("logs").Value() - before; got != 0 {
		t.Fatalf("dropped %v batches: a payload meeting a RESPONDED transient error (401 on a rotating "+
			"bearer token, 404 through a collector rollout) was rotated into drop evidence instead of "+
			"being retried — after only %d attempts, where stuckAfterAttempts is %d", got, tries, stuckAfterAttempts)
	}
	if !delivered {
		t.Fatalf("the payload was never delivered (%d attempts) although attempt %d would have succeeded",
			tries, stuckAfterAttempts)
	}
	if tries != stuckAfterAttempts {
		t.Fatalf("the payload cost %d attempts, want exactly %d: its first lap must be the full backed-off "+
			"cycle, so a transient condition lasting %d attempts is delivered on the next one",
			tries, stuckAfterAttempts, stuckAfterAttempts-1)
	}
}

// The outage half, which the short lap must not weaken: with the collector
// refusing EVERYTHING — a memory-limiter overload answers ResourceExhausted to
// every batch — nothing is poison, nothing may be dropped, and the backed-off
// cycle must still be what paces the retries.
func TestTotalRefusalDropsNothingAndKeepsTheBackedOffCycle(t *testing.T) {
	send := &poisonSender{refuseAl: true}
	dir := t.TempDir()
	ls, err := OpenBuffer(dir+"/logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	b := NewBuffered(send, ls, nil, nil, 5*time.Millisecond, quietLogger())

	for i := 0; i < 3; i++ {
		if err := b.ExportLogs(context.Background(), logsWith("good")); err != nil {
			t.Fatal(err)
		}
	}
	before := obs.BufferDroppedBatches.WithLabelValues("logs").Value()

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	b.Run(ctx)

	if got := obs.BufferDroppedBatches.WithLabelValues("logs").Value() - before; got != 0 {
		t.Fatalf("dropped %v batches while the collector was refusing EVERYTHING: that is an outage, "+
			"not poison, and the buffer exists to survive it", got)
	}
	if attempts, _ := send.counts(); attempts < stuckAfterAttempts {
		t.Fatalf("only %d attempts in the whole window: the first cycle must still be the full backed-off one", attempts)
	}
}

// The unit rules behind it, so a change to either condition is visible here
// rather than as a timing wobble in the integration tests above.
func TestShortLapNeedsBothPayloadEvidenceAndAResponse(t *testing.T) {
	newSink := func() *sink[plog.Logs] {
		return &sink[plog.Logs]{kind: "logs", log: quietLogger(), backoff: time.Millisecond}
	}
	for _, tc := range []struct {
		name        string
		accountable bool
		err         error
		want        int
	}{
		// A lap the drop rule is already counting, and the collector ANSWERING
		// about this payload: one attempt, then the head is released.
		{"accountable lap, collector refuses this payload", true, status.Error(codes.ResourceExhausted, "too large"), 1},
		{"accountable lap, collector answers Internal", true, status.Error(codes.Internal, "bug"), 1},
		// Back-pressure and transport failures say nothing about the payload, so
		// they keep the full backed-off cycle however much evidence exists.
		{"accountable lap, collector back-pressures", true, status.Error(codes.Unavailable, "restarting"), stuckAfterAttempts},
		{"accountable lap, collector times out", true, context.DeadlineExceeded, stuckAfterAttempts},
		{"accountable lap, collector answers 503", true, &HTTPStatusError{Code: 503}, stuckAfterAttempts},
		{"accountable lap, collector answers 429", true, &HTTPStatusError{Code: 429}, stuckAfterAttempts},
		// No evidence about THIS payload — a first sighting, or a lap no delivery
		// spans. Nothing may be held against it, whatever the collector answers:
		// this is the row that keeps a rotating token's 401 and a rollout's 404
		// from costing good data its retry depth.
		{"no payload evidence, responded", false, status.Error(codes.ResourceExhausted, "too large"), stuckAfterAttempts},
		{"no payload evidence, 401 on a rotating token", false, status.Error(codes.Unauthenticated, "bad token"), stuckAfterAttempts},
		{"no payload evidence, 404 through a rollout", false, &HTTPStatusError{Code: 404}, stuckAfterAttempts},
		{"no payload evidence, transport", false, context.DeadlineExceeded, stuckAfterAttempts},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSink()
			attempts := 0
			got := s.trySend(context.Background(), func(context.Context) error {
				attempts++
				return tc.err
			}, tc.accountable)
			if got != sendStuck {
				t.Fatalf("trySend = %v, want sendStuck", got)
			}
			if attempts != tc.want {
				t.Fatalf("attempts = %d, want %d", attempts, tc.want)
			}
		})
	}
}

// A first sighting is never accountable, however many batches the collector has
// accepted. This is the unit statement of the regression: the old gate was
// exactly "s.delivered has moved", which is true here.
func TestFirstSightingIsNeverAccountableHoweverWarmTheSink(t *testing.T) {
	s := &sink[plog.Logs]{kind: "logs", log: quietLogger(), backoff: time.Millisecond}
	s.delivered = 1000 // a busy, demonstrably-live collector
	data := []byte("a good batch meeting a transient condition")

	if s.accountableLap(data) {
		t.Fatal("a payload that has never failed a lap has no poison evidence: its first cycle must be full")
	}
	attempts := 0
	got := s.trySend(context.Background(), func(context.Context) error {
		attempts++
		if attempts < stuckAfterAttempts {
			return status.Error(codes.Unauthenticated, "token rotating")
		}
		return nil
	}, s.accountableLap(data))
	if got != sendOK {
		t.Fatalf("trySend = %v after %d attempts, want sendOK: a condition lasting %d attempts is delivered on the next",
			got, attempts, stuckAfterAttempts-1)
	}
	if attempts != stuckAfterAttempts {
		t.Fatalf("attempts = %d, want %d", attempts, stuckAfterAttempts)
	}
}

// The self-limiting property: the evidence is re-established every lap, so a
// second short lap has to be earned by a fresh delivery of something else.
// Without it, a queue holding nothing but a refused payload would spin on it at
// wire speed. stuckTooLong is what re-stamps it, so the two are driven together
// here exactly as drainLoop drives them.
func TestShortLapMustBeEarnedByAFreshDelivery(t *testing.T) {
	s := &sink[plog.Logs]{kind: "logs", log: quietLogger(), backoff: time.Millisecond}
	s.delivered = 7 // the collector accepted plenty before this batch ever failed
	data := []byte("a poison batch")

	lap := func() int {
		t.Helper()
		attempts := 0
		got := s.trySend(context.Background(), func(context.Context) error {
			attempts++
			return status.Error(codes.ResourceExhausted, "too large")
		}, s.accountableLap(data))
		if got != sendStuck {
			t.Fatalf("trySend = %v, want sendStuck", got)
		}
		s.stuckTooLong(data) // what drainLoop does with a sendStuck, and what stamps the evidence
		return attempts
	}

	if n := lap(); n != stuckAfterAttempts {
		t.Fatalf("first lap attempts = %d, want the full cycle %d", n, stuckAfterAttempts)
	}
	if n := lap(); n != stuckAfterAttempts {
		t.Fatalf("second lap attempts = %d with no delivery since the first: the head is holding nothing "+
			"up, and a queue with nothing else to drain would spin on it at wire speed", n)
	}
	s.delivered++ // another batch got through while this one circled
	if n := lap(); n != 1 {
		t.Fatalf("attempts after a fresh delivery = %d, want 1: this lap is spending the poison budget, "+
			"so the remaining attempts only hold the shared queue head", n)
	}
	if n := lap(); n != stuckAfterAttempts {
		t.Fatalf("attempts = %d immediately after a short lap, want the full cycle %d: the evidence must be "+
			"re-earned every lap", n, stuckAfterAttempts)
	}
}
