package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// store.WaiterCostBytes is what the blocked-lookup cap SPENDS per waiter, and
// the cap is that budget divided by it. Nothing in internal/store allocates any
// of that cost — it is a parked net/http handler — so the store cannot check its
// own number, and the version of this check that lived there restated the
// constants instead of measuring, which is how it came to agree with a figure
// that was off by 8x.
//
// Three successive versions of this file then measured a LIST of shapes, and
// every time the list was the defect: the shape it did not think of parked at
// several times what it budgeted. The last of the three is the instructive one,
// because it had a general half and the general half was blind — it fuzzed the header block
// behind a HARD-CODED request line, and measured through http.ReadRequest, which
// never runs ServeMux. So it covered exactly the term releaseParkedHead drops
// and excluded the one it keeps, and a 12 KiB URI parked at 93% of the budget
// undetected.
//
// So the argument is made in two halves that between them do not depend on
// anyone guessing an attack, and each half measures through the REAL serving
// path:
//
//   - what a parked request retains is bounded by a floor plus its
//     If-None-Match validator, whatever the sender sent — that is the ONE term
//     releaseParkedHead deliberately keeps at the sender's size, and
//     FuzzParkedLookupRetainsNothingButItsValidator fuzzes the request TARGET
//     and the header block against it through the real mux and the real handler.
//   - the wire is bounded by maxAdmittedHead, which net/http enforces itself
//     with a 431 before any handler runs.
//
// This test is the end-to-end half: real requests on a real listener, parked,
// measured. It is the one that sees the term the handler CANNOT release — the
// connection keeps the request LINE alive in http.conn.lastMethod, which is
// r.Method, which is a slice of it — and its per-shape ceiling is therefore
// computed from each shape's own wire (see hostileShape.keeps), never
// hand-typed.
//
// Every request it measures arrives on a REUSED connection, because that is
// where the wire bound is widest: see maxAdmittedHead.
//
// What it measures is RETAINED HEAP, and the floor it subtracts is measured
// immediately before each shape rather than once up front. Both of those are
// the fix for a version of this test that could not be run twice in one
// process. That one took its floor once and every shape afterwards, so the
// floor was the one measurement taken in a cold process: it read 29,237 B on
// the first iteration of a `-count=2` run and 15,789-19,331 B on the second, a
// ~13 KB swing against the 8 KiB of slack it allowed for the movement — while a
// SHAPE measured on both iterations moved by under a kilobyte (35,224 B and
// 36,207 B). A ceiling that subtracts a term noisier than its own tolerance
// fails on whichever shape the floor happens to land under, so it failed on
// every `-count=2` attempt and it failed on honest shapes: releaseParkedHead
// was doing its job the whole time.
//
// So a measurement is only ever compared with another measurement taken in the
// same state: see parkAndMeasure for the two things that make that state
// repeatable (a JOINED teardown and a warmed process), and retainedHeap for why
// the goroutine stacks are allowed for rather than measured.
//
// Every assertion below is a CEILING, which is the shape a cost budget wants
// and also the shape that passes when the measurement stops working. The three
// things that keep the ceilings honest are therefore not ceilings at all, and
// they live in parkAndMeasure: a floor under the measured figure
// (plausibleParkedHeap), a count of the goroutines the stack allowance is
// paying for (parkedStackGoroutines), and a dial that waits out a neighbour's
// socket churn instead of skipping the whole measurement on it
// (retryingDialer).
func TestParkedLookupCostFitsItsBudget(t *testing.T) {
	if testrace.Enabled {
		// The detector adds shadow memory and per-allocation bookkeeping to every
		// one of these figures, which would fail a budget nothing had regressed.
		t.Skip("memory measurement is meaningless under -race")
	}
	// Enough parked lookups that the per-waiter figure is well clear of the test
	// binary's own noise (200 x 14 KB is ~3 MB of signal), and few enough that
	// the two file descriptors each one costs cannot exhaust a modest ulimit.
	const n = 200

	// The margin the budget has to keep over the measured cost. RSS per parked
	// lookup measured 1.20-1.25x the retained heap this test reports plus the
	// stack it allows for, and RSS is what evicts a pod. 1.15 is deliberately
	// BELOW the observed RSS overhead — the job here is to refuse a budget that
	// has no headroom at all (a 52 KiB budget against a 51 KB worst case, would
	// pass a bare <= and is exactly the accident this catches), not to pin a
	// ratio that is itself noisy. Written as a fraction so the arithmetic stays
	// integral.
	const marginNum, marginDen = 115, 100

	poll := hostileShape{block: ordinaryPollHeaders}

	// The first park round in a process reads ~1.5 KB per waiter high — first
	// touch of the pages, the spans and the dial path — and every round after it
	// agrees with the next to within ~500 B. Since the whole argument below is a
	// DIFFERENCE between two rounds, the one round that is not like the others
	// must not be either of them: this one is thrown away so that both are warm.
	parkAndMeasure(t, n, poll)

	base := parkAndMeasure(t, n, poll)
	t.Logf("an agent's poll: %d B of retained heap per parked lookup, plus %d B allowed for its stack, "+
		"budget %d B", base, parkedStackAllowance, store.WaiterCostBytes)

	// The budget's whole derivation, in one assertion and independent of any
	// shape anyone thought of: the worst admissible parked lookup is an ordinary
	// poll plus ONE wire. keeps() is the request line plus the validator, and
	// both live in the one head, so no shape can spend the admission twice —
	// which makes maxAdmittedHead an upper bound on every shape's keeps(), the
	// ones below and the ones nobody wrote. A shape list can miss a shape; this
	// cannot. (It replaced a bare "the poll must be under half the budget",
	// which was a proxy for this and said nothing about what the other half had
	// to cover.)
	if want := (base + parkedStackAllowance + maxAdmittedHead) * marginNum / marginDen; want > store.WaiterCostBytes {
		t.Fatalf("an ordinary agent poll parks at %d B of heap, and with %d B of stack and one whole %d-byte "+
			"admissible head it needs %d B of budget once the margin is applied, against the %d B "+
			"store.WaiterCostBytes carries: the cap has no headroom left for what a sender can add to a poll",
			base, parkedStackAllowance, maxAdmittedHead, want, store.WaiterCostBytes)
	}

	for _, shape := range hostileShapes() {
		t.Run(shape.name, func(t *testing.T) {
			// The floor, measured HERE: adjacent to the shape and in the same
			// warm process, because it is subtracted from the shape and a floor
			// taken in a different state is subtracted from nothing.
			base := parkAndMeasure(t, n, poll)
			cost := parkAndMeasure(t, n, shape)
			t.Logf("%d parked lookups, %d B of retained heap each, against a ceiling of %d B (an ordinary "+
				"poll measured alongside at %d B, the %d wire bytes this shape may keep, and %d B of slack); "+
				"stack allowance %d B, budget %d B\n  (%s)",
				n, cost, base+shape.keeps()+costSlack, base, shape.keeps(), costSlack,
				parkedStackAllowance, store.WaiterCostBytes, shape.source)

			// The per-shape ceiling: an ordinary poll, plus the wire bytes this
			// shape is expected to leave retained, plus slack. keeps() is
			// COMPUTED from the shape's own request line and validator — the two
			// terms that survive a park and the two that a new shape could
			// otherwise be given a wrong hand-typed allowance for. Everything
			// else about the head has to cost nothing at all: these shapes
			// retain 31-433 KB when the head is HELD.
			if want := base + shape.keeps() + costSlack; cost > want {
				t.Fatalf("a parked lookup in this shape retains %d B of heap against %d B for an ordinary poll "+
					"measured beside it, over the %d B ceiling (the poll, plus the %d wire bytes this shape may "+
					"leave retained: its request line, which the CONNECTION pins, and its If-None-Match "+
					"validator, which releaseParkedHead keeps on purpose): something else in the head is "+
					"surviving the park, so the waiter cap is spending a number the sender chooses",
					cost, base, want, shape.keeps())
			}

			// The absolute half. The heap is measured; the goroutine stack is
			// charged at an allowance that is above every honest reading of it
			// (retainedHeap says why it is not measured), so this arithmetic can
			// only be stricter than the truth, never kinder.
			total := cost + parkedStackAllowance
			if total > store.WaiterCostBytes {
				t.Fatalf("a parked lookup costs %d B in this shape (%d B of retained heap and %d B allowed "+
					"for its stack), over the %d B store.WaiterCostBytes budgets for it: a saturated cap holds "+
					"%d MiB rather than the %d MiB the cap was derived from, so the count bounds a number and "+
					"not the process",
					total, cost, parkedStackAllowance, store.WaiterCostBytes,
					(int64(total)*int64(store.DefaultMaxWaiters))>>20, int64(store.WaiterBudgetBytes)>>20)
			}
			// …and it has to fit with the margin the budget was rounded up FOR,
			// or the number is right only by luck: a budget that just covers the
			// mean of one metric is not covering the thing it is spent on.
			if want := total * marginNum / marginDen; want > store.WaiterCostBytes {
				t.Fatalf("a parked lookup costs %d B, which needs %d B of budget once the margin is applied, "+
					"but store.WaiterCostBytes is %d B: the cap is derived from a cost with no room for the "+
					"run-to-run variance and the RSS overhead that were measured alongside it",
					total, want, store.WaiterCostBytes)
			}
		})
	}
}

// costSlack is how far above "an ordinary poll plus this shape's keeps()" a
// measurement may land before the difference is called a regression.
//
// It is sized from three measured quantities, not chosen for comfort:
//
//   - the noise. 120 measurements across twelve processes put an ordinary poll
//     between 14,261 and 14,834 B of retained heap, and every shape's
//     (cost - poll - keeps()) between -997 and +705 B.
//   - the allocator's rounding. keeps() counts the sender's bytes; what is
//     retained is a size class. Above 8 KiB those step by up to 2048 B, which is
//     where the positive residual above comes from.
//   - what it must not hide. With releaseParkedHead made a no-op, the six
//     shapes it is the defence for exceed their keeps() by 16.4 KB, 22.0 KB,
//     48.5 KB, 122 KB, 251 KB and 419 KB. (The other four spend their whole
//     retention on the request line, which keeps() allows for either way, and
//     say so by not moving.)
//
// So 4 KiB is a whole size-class step with room to spare — nearly six times the
// worst residual measured — and still four times below the smallest regression
// it exists to catch. (The 8 KiB it replaced was covering the FLOOR moving,
// which is now measured in the same state as the shape instead of being allowed
// for here.)
const costSlack = 4096

// parkedStackAllowance is what a parked lookup is charged for its goroutine
// stack. It is the one term of the cost this test does not measure, and the
// reason is in retainedHeap: the runtime's stack accounting cannot be
// attributed to a park round. 16 KiB is the next power of two above every
// honest reading of it, so charging it can only make the budget assertions
// stricter than the truth.
const parkedStackAllowance = 16 << 10

// parkedStackGoroutines is the number of goroutines per parked lookup that
// parkedStackAllowance is sized for: net/http's conn goroutine, grown to 8 KiB
// by the parse, and the background-read goroutine that conn starts before it
// calls the handler (server.go's serve: startBackgroundRead, or
// registerOnHitEOF when a body remains).
//
// Charging a flat allowance instead of measuring the stacks is the right call
// and retainedHeap argues it, but it converts one of the two terms of the cost
// from a measurement into an ASSUMPTION — and specifically into an assumption
// about a COUNT that nothing counted. A change that parked a THIRD goroutine
// per lookup (a per-request watchdog, a cancel propagator, a body drainer)
// would put ~8 KiB a waiter beyond the allowance, and every number this test
// prints would be unchanged, because heap is all it reads.
//
// So parkAndMeasure counts them, at the instant it takes the heap snapshot and
// against the same quiescent baseline its join already uses. The check is an
// UPPER bound only: the two Transfer-Encoding shapes park exactly ONE goroutine
// each, because a request whose body remains defers the background read to that
// body's EOF, and fewer goroutines than the allowance pays for is the direction
// in which the allowance is safe.
const parkedStackGoroutines = 2

// goroutineSlack is what that count tolerates for goroutines that are not
// per-waiter. The listener's Serve loop is already inside the baseline
// (quiescent is taken after it exists), so this covers only the runtime or the
// test binary adding one between the two reads.
//
// 880 measurements across forty processes read the delta as exactly
// parkedStackGoroutines x n (795 of them), one short of it (5, a background
// reader not yet started when the snapshot was taken), or exactly n (40, the
// two chunked shapes) — and never once above it. 16 is therefore pure
// insurance, and it is more than an order of magnitude below the 200 that one
// extra goroutine per waiter would add here.
const goroutineSlack = 16

// FuzzParkedLookupRetainsNothingButItsValidator is the half of the argument that
// does not depend on anyone guessing an attack.
//
// The property: while a lookup is parked, what its request still retains is a
// small floor plus at most twice its own If-None-Match validator — and NOTHING
// that scales with anything else the sender sent. The validator is the one term
// releaseParkedHead keeps at the sender's size (dropping it would turn every
// agent's 304 into a full document), so it is the one term this bound has to
// name; the request target, the header block and the trailer list must all cost
// zero.
//
// It fuzzes the request TARGET as well as the header block, and it serves both
// through the real mux and the real handler, because the version before it
// did neither: net/http's parse of a head makes three to five copies of
// the request line — r.Method, r.RequestURI, r.URL.RawPath and r.URL.RawQuery
// are all SLICES of the one string net/textproto read, url.setPath unescapes a
// second copy, and ServeMux's multi-wildcard match pathUnescapes a third into
// r.matches, which only exists when a request goes through a mux and only
// releases through SetPathValue.
//
// It measures HEAP only, and no sockets: the goroutine stacks and the
// connection buffers are this process's floor, not the sender's term, and the
// end-to-end test above is where they are counted.
func FuzzParkedLookupRetainsNothingButItsValidator(f *testing.F) {
	const id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	f.Add("/v1/containers/"+id+"?wait=600s", ordinaryPollHeaders)
	for _, s := range hostileShapes() {
		f.Add(s.target(id), s.block)
	}
	// Shapes worth handing the fuzzer as starting points.
	f.Add("/v1/containers/"+id, "")
	f.Add("/v1/containers/containerd%3A%2F%2F"+id, "")                                 // an escaped runtime prefix
	f.Add("/v1/containers/"+strings.Repeat("a/", 2000)+":"+id, "")                     // many segments
	f.Add("/v1/containers/"+id, "X-A: v\r\n"+strings.Repeat(" c\r\n", 200))            // folded continuations
	f.Add("/v1/containers/"+id, "Transfer-Encoding: chunked\r\nContent-Length: 5\r\n") // conflicting framing
	f.Add("/v1/containers/"+id, strings.Repeat("Cookie: a=b\r\n", 200))                // an interned key
	f.Add("/v1/containers/"+id, "Trailer: "+commaKeyList(2048)+"\r\n")                 // a trailer list, no chunking
	f.Add("/v1/containers/"+id, strings.Repeat("X-A: \xff\xfe\r\n", 200))              // non-UTF-8 values
	f.Add("/v1/containers/"+id, "X-A:"+strings.Repeat(" ", 2048)+"v\r\n")              // a value net/textproto trims

	f.Fuzz(func(t *testing.T, target, block string) {
		if testrace.Enabled {
			t.Skip("memory measurement is meaningless under -race")
		}
		head := "GET " + target + " HTTP/1.1\r\nHost: h\r\n" + block + "\r\n"
		if len(head) > maxAdmittedHead {
			// Over what the server admits even on the connection shape that
			// admits the most (maxAdmittedHead): net/http answers 431 without
			// reading it, and nothing parks.
			t.Skip("over the byte bound")
		}
		got, validator, ok := retainedWhileParked(t, head, 64)
		if !ok {
			// Not a request that reaches the parking handler at all: a head
			// net/http rejects, a route that does not match, an id the handler
			// 400s, a wait budget of zero. None of those hold anything.
			t.Skip("this request does not park")
		}
		// The floor is the Request struct, the URL, the clipped copies of the
		// method/proto/target, the response writer and the allocator's rounding:
		// measured 1.2-2.3 KB across every shape tried, over ten runs. 4096 is
		// that with room for a Go release moving a struct, not a licence for a
		// new retained container.
		const floor = 4096
		if want := floor + 2*len(validator); got > want {
			// The head can be 16 KiB; a fuzz failure writes the whole input to
			// testdata/fuzz, so the message carries only enough to recognise it.
			t.Fatalf("a parked lookup retains %d B for a %d-byte request whose validator is %d B, over the "+
				"%d B a floor plus twice that validator allows: something in the parse is still held across "+
				"the park and it scales with what the sender sent, which is what makes the blocked-lookup cap "+
				"bound a count instead of the process\ntarget starts: %q\nblock starts: %q",
				got, len(head), len(validator), want,
				target[:min(len(target), 160)], block[:min(len(block), 160)])
		}
	})
}

// The header block an agent's own client sends.
const ordinaryPollHeaders = "User-Agent: kubescrape-agent/1.0\r\n" +
	"Accept-Encoding: gzip\r\nIf-None-Match: \"0123456789abcdef\"\r\n"

// keepAlivePrelude is the cheap request every connection in this file sends
// FIRST, so that the request under measurement arrives on a reused connection.
// It is written in the same syscall as the request behind it, which is what
// leaves part of that request already sitting in the connection's bufio reader
// (see maxAdmittedHead). /healthz is served without touching the store, so
// nothing about it parks or allocates.
const keepAlivePrelude = "GET /healthz HTTP/1.1\r\nHost: h\r\n\r\n"

// maxAdmittedHead is the largest request head this server actually serves, and
// it is NOT maxHeaderBytes, nor maxHeaderBytes plus net/http's documented 4 KiB
// slop. Three terms, of which only the first two are visible from
// Server.initialReadLimitSize:
//
//	maxHeaderBytes                        8192   the configured bound
//	+ net/http's bufio slop               4096   initialReadLimitSize adds it
//	+ what the PREVIOUS request left buffered
//	  (the connection's 4096-byte bufio reader, less the prelude's own bytes)
//	                                      4062
//	                                    ------
//	                                     16350
//
// The third term is the one a fresh-connection measurement cannot see: the read
// limit is set on the connReader, which counts bytes read FROM THE SOCKET, and
// a fill issued while the PREVIOUS request's head was being parsed can pull up
// to a whole bufio buffer of the next head in before that limit is reset. Those
// bytes are never charged. Measured against this package's real listener: a
// fresh connection admits exactly 12288 and 431s at 12289, while behind this
// 34-byte prelude the same server admits 16350 — and the figure tracks
// 12288 + 4096 - len(prelude) across prelude sizes from 34 to 1007.
//
// It is a LOWER BOUND on the admission and not the exact byte the server 431s
// at, which is why the test that checks it checks it with a margin: net/http's
// connReader.backgroundRead reads one byte straight off the socket while the
// previous request's handler runs and parks it in connReader.byteBuf, and
// connReader.Read then hands that byte out without charging it against the new
// limit. Whether it lands is a race with abortPendingRead, so a head of
// maxAdmittedHead+1 is admitted sometimes and refused others — measured 28, 30
// and 28 times in 200 at +1 for preludes of 34, 100 and 1007 bytes, and 0 in
// 200 at +2 for all three. Sizing the shapes at maxAdmittedHead is therefore
// right, because that size is always admitted; asserting a refusal one byte
// above it is not, and failed three runs in ten.
//
// So the wire term of a parked lookup is ~16 KB, not ~12 KB, and every shape
// here is sized against it (TestTheHarnessMeasuresEveryHeadTheServerAdmits
// checks both directions against the real listener rather than trusting this
// arithmetic).
const maxAdmittedHead = maxHeaderBytes + 4096 + 4096 - len(keepAlivePrelude)

// refusedHead is a head this server refuses however the reads land, and it is
// what the high direction below probes instead of maxAdmittedHead+1.
//
// The margin is one whole bufio buffer, and it comes from the fill mechanics
// rather than from a measurement. Everything that can widen the admission
// beyond the charged maxHeaderBytes+4096 is uncharged bytes of two kinds: what
// a fill left sitting in the connection's bufio reader when the limit was reset
// — at most one whole buffer, 4096, which is more than the 4096-len(prelude)
// this connection shape leaves, because a buffer drained below four bytes is
// refilled by the Peek(4) net/http does between requests while the limit is
// still infinite — and the single background-read byte above. So no connection
// shape can admit more than maxHeaderBytes+4096+4096+1 = 16385, and this sits a
// further 4061 bytes clear of that ceiling while still failing on any drift
// that moves the admission by a buffer or more.
const refusedHead = maxAdmittedHead + 4096

// The harness's shapes are only worth what their wire bound is worth: a bound
// below the server's real admission hides a whole class of request from BOTH
// halves of the argument (nothing here would fail, the shapes would just be
// smaller than what a sender can send), and a bound above it turns every shape
// into a 431 that never parks. So both directions are checked against the real
// listener rather than read off net/http's internals — the low one at the
// constant itself, which has to be admitted, and the high one at refusedHead,
// with a margin, because the admission's last byte is a race (see
// maxAdmittedHead) and the version of this test that pinned it exactly failed
// three runs in ten.
func TestTheHarnessMeasuresEveryHeadTheServerAdmits(t *testing.T) {
	srv := newAPI(store.New(time.Minute), time.Second).HTTPServer(":0")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	// A head of exactly n bytes for an id an empty store answers 404 for
	// without parking, padded out with one header value.
	head := func(n int) string {
		req := fmt.Sprintf("GET /v1/containers/%064x?wait=0 HTTP/1.1\r\nHost: h\r\nX-P: \r\n\r\n", 1)
		if n < len(req) {
			t.Fatalf("cannot build a %d-byte head; the shortest is %d", n, len(req))
		}
		return strings.Replace(req, "X-P: ", "X-P: "+strings.Repeat("p", n-len(req)), 1)
	}
	// admitted reports whether a head of n bytes reaches a handler when it
	// arrives behind keepAlivePrelude on the same connection.
	//
	// It dials through retryingDialer for the reason given there: this is the
	// check that the shapes are sized against the server's REAL admission, and
	// a skip on a neighbour's socket churn would delete it while reporting the
	// same green as a run that made it.
	dialer := &retryingDialer{t: t, addr: ln.Addr().String(), need: 1}
	t.Cleanup(dialer.report)
	admitted := func(n int) bool {
		c := dialer.dial()
		defer func() { _ = c.Close() }()
		// An RST teardown, as in parkAndMeasure: the binary search below dials
		// tens of times and the ephemeral range is shared with every other test
		// in the package.
		if tcp, ok := c.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = c.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := c.Write([]byte(keepAlivePrelude + head(n))); err != nil {
			t.Fatalf("write: %v", err)
		}
		br := bufio.NewReader(c)
		first, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("the prelude was not answered: %v", err)
		}
		_ = first.Body.Close()
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("no response to the %d-byte head behind the prelude: %v", n, err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge
	}

	if !admitted(maxAdmittedHead) {
		t.Fatalf("a %d-byte head behind a %d-byte prelude is refused, so the shapes measured here are "+
			"sized ABOVE what this server admits: they 431 instead of parking and the cost measurement is "+
			"of nothing", maxAdmittedHead, len(keepAlivePrelude))
	}
	if admitted(refusedHead) {
		// Search for what the admission actually is, so the failure carries the
		// number to write down rather than only the fact that the old one is
		// wrong. The right-hand end is FOUND rather than assumed: whatever
		// moved the admission past a whole uncharged buffer may have moved it
		// past several, and a search whose end is itself admitted reports a
		// number that is merely where the search started.
		lo, hi := refusedHead, refusedHead+4096 // lo admitted, hi to be refused
		for admitted(hi) {
			if hi > 1<<20 {
				t.Fatalf("this server admits a head of %d bytes behind a %d-byte prelude: the read limit "+
					"net/http applies to a request head is not bounding it at all, so neither is "+
					"maxHeaderBytes", hi, len(keepAlivePrelude))
			}
			lo, hi = hi, hi*2
		}
		for hi-lo > 1 {
			if mid := (lo + hi) / 2; admitted(mid) {
				lo = mid
			} else {
				hi = mid
			}
		}
		// "at least": the last byte of the admission is the background-read
		// race described at maxAdmittedHead, so the search can land either side
		// of it.
		t.Fatalf("this server admits a head of at least %d bytes behind a %d-byte prelude, %d over the %d "+
			"maxAdmittedHead is sized for: every shape here is sized against that constant, so a whole "+
			"class of admissible request — the widest one there is — is measured by neither half of this "+
			"argument", lo, len(keepAlivePrelude), lo-maxAdmittedHead, maxAdmittedHead)
	}
}

// retryingDialer opens the connections this file measures on, surviving the
// port churn a NEIGHBOURING test leaves behind instead of skipping on it.
//
// What it replaces was `t.Skipf("could not connect: …")` on the first failed
// dial, in both places that dial — which is this file's own subject matter
// moved into the harness: a skip deletes the only end-to-end measurement of the
// waiter budget and reports exactly what a run that measured all of it reports.
// No assertion is weakened and nothing is red. Nothing distinguishes them.
//
// And it is reachable without anything being wrong here at all.
// TestServerConcurrentLoad, an unmodified test in this package, drives 32
// goroutines through a keep-alive-less client for two seconds and leaves ~3,700
// sockets in TIME_WAIT per run; a whole-package `-count=5` puts 32,456 of them
// against the 28,232 ports of this machine's ephemeral range (32768-60999) —
// measured at 32,989 on the run that checked — and the next dial anywhere in
// the binary gets EADDRNOTAVAIL. It is the likeliest explanation for a
// `-count=5` failure seen here once and never reproduced. (This test's own
// sockets are not the problem: they are torn down with an RST, see SetLinger
// below.)
//
// Of the two remedies — survive it, or report it as its own visible outcome —
// this takes the first, and keeps a FAILURE rather than a skip for the case it
// cannot survive. Retrying is available because the condition is self-clearing
// and on a known schedule: Linux holds TIME_WAIT for a fixed 60 s
// (TCP_TIMEWAIT_LEN, not tunable), and the ports come back on the same schedule
// they were spent on, so waiting out one whole generation of them turns a
// neighbour's churn into a pause in this test rather than an outcome of it. A
// dial that is still failing after that is not port churn draining, and the
// honest report of THAT is a failure naming the cause — because the other
// remedy on the table, a distinct and loudly-worded skip, is still a skip, and
// a green run is what a reader takes from it.
//
// Retries are counted rather than swallowed, so a run that was slowed by a
// neighbour says so in its log instead of looking like a slow measurement.
type retryingDialer struct {
	t    *testing.T
	addr string
	// need is how many connections the caller will ask for, for the failure
	// message only.
	need    int
	retries int
	waited  time.Duration
}

// dial returns a connection, or fails the test if the machine cannot give it
// one within a whole TIME_WAIT generation.
func (d *retryingDialer) dial() net.Conn {
	d.t.Helper()
	// One TIME_WAIT generation plus a little: long enough that a full ephemeral
	// range spent by a sibling has recycled, short enough that a genuinely
	// unreachable listener is reported inside `go test`'s own budget.
	const budget = 75 * time.Second
	start := time.Now()
	deadline := start.Add(budget)
	delay := time.Millisecond
	for attempt := 1; ; attempt++ {
		c, err := net.Dial("tcp", d.addr)
		if err == nil {
			if attempt > 1 {
				d.retries++
				d.waited += time.Since(start)
			}
			return c
		}
		if time.Now().After(deadline) {
			d.t.Fatalf("no connection to %s after %d attempts over %v: %v\n"+
				"this is the end-to-end measurement of what a parked lookup costs, and it needs its "+
				"connections: the failure is reported rather than skipped because a skip here is "+
				"indistinguishable from a run that measured everything. An ephemeral range exhausted by a "+
				"sibling test's TIME_WAIT sockets recycles inside %v and is already waited out, so this is "+
				"a file-descriptor limit, a listener that is not there, or a range far smaller than the "+
				"%d connections this caller opens",
				d.addr, attempt, time.Since(start).Round(time.Millisecond), err, budget, d.need)
		}
		time.Sleep(delay)
		if delay < 250*time.Millisecond {
			delay *= 2
		}
	}
}

// report logs what the retries cost, so that a measurement slowed by a
// neighbour's socket churn is visible as that rather than as noise.
func (d *retryingDialer) report() {
	d.t.Helper()
	if d.retries > 0 {
		d.t.Logf("%d dials did not connect first time and %v was spent waiting for the ephemeral range to "+
			"recycle: a neighbour's TIME_WAIT sockets, survived rather than skipped on",
			d.retries, d.waited.Round(time.Millisecond))
	}
}

// hostileShape is a request chosen to be expensive to parse: path text before
// the container id, extra query, and extra header lines.
type hostileShape struct {
	name string
	// prefix is path text placed BEFORE the container id. The id stays the last
	// colon-separated field of the path, which is what kubemeta.NormalizeContainerID
	// returns — so a shape may make the PATH arbitrarily long while the id the
	// handler sees is a well-formed 64-hex one that parks.
	prefix string
	query  string
	block  string
	source string
}

// target renders the shape's request target for one container id.
func (s hostileShape) target(id string) string {
	return "/v1/containers/" + s.prefix + id + "?wait=600s" + s.query
}

// keeps is how many of the sender's bytes this shape may still have retained
// while it is parked. It is COMPUTED rather than declared, because the rule is
// the same for every shape and a hand-typed number is what let an earlier
// version of this test pass a shape sitting at 98% of the budget:
//
//   - the whole REQUEST LINE, because net/textproto reads it as one string and
//     r.Method is a slice of it — and http.conn.lastMethod holds that slice for
//     the life of the connection, where no handler can reach it. This is the
//     term releaseParkedHead cannot drop, only refuse to multiply.
//   - the If-None-Match validator, which releaseParkedHead keeps on purpose.
//
// Everything else — every other header, the trailer list, the map capacity
// net/textproto pre-sized from the line count, the unescaped copies of the path
// — must cost nothing.
func (s hostileShape) keeps() int {
	line := len("GET " + s.target(strings.Repeat("0", 64)) + " HTTP/1.1")
	return line + len(headerValue(s.block, "If-None-Match"))
}

// headerValue is the raw value of one header line in a block, or "".
func headerValue(block, key string) string {
	for line := range strings.SplitSeq(block, "\r\n") {
		if v, ok := strings.CutPrefix(line, key+": "); ok {
			return v
		}
	}
	return ""
}

func hostileShapes() []hostileShape {
	// What a client may actually send, on the connection shape that admits the
	// most of it (maxAdmittedHead), less the request line and the framing every
	// shape carries.
	const room = maxAdmittedHead - 300

	return []hostileShape{{
		name:   "widest distinct-key block",
		block:  widestHeaderBlock(room),
		source: "the shape a byte bound alone does not bound: 2903 minimal header lines, 266 KB of retained heap when the head is HELD",
	}, {
		name:   "one key repeated",
		block:  strings.Repeat("x:\r\n", 1024),
		source: "len(r.Header) reports 1. 4 KiB of wire, 137 KB of retained heap when the head is HELD: net/textproto pre-sizes the map from the LINE count",
	}, {
		name:   "trailer list",
		block:  "Transfer-Encoding: chunked\r\nTrailer: " + commaKeyList(room) + "\r\n",
		source: "net/http DELETES Trailer and Transfer-Encoding from r.Header, so a two-line block counts as ZERO and retains 433 KB of heap when the head is HELD",
	}, {
		name:   "trailer lines, one key",
		block:  "Transfer-Encoding: chunked\r\n" + strings.Repeat("Trailer: a\r\n", 1000),
		source: "nothing at all left to price — one trailer key, no header keys — and 36 KB of retained heap when the head is HELD, all of it the map capacity the deleted lines paid for",
	}, {
		name:   "fat single value",
		block:  "X-P: " + strings.Repeat("v", room) + "\r\n",
		source: "one line, all bytes: the shape a LINE count would wave through",
	}, {
		// The escape that motivated computing keeps() from the
		// request line: the path is not the container id. NormalizeContainerID
		// returns what follows the LAST colon, so maxContainerIDLen never saw
		// these 16 KiB, the id parked normally, and the %-escape bought a second
		// and a third copy of the path (url.setPath's unescape, ServeMux's
		// pathUnescape into r.matches). With the head HELD it retains 63 KB of
		// heap, which with its stack is past the whole 64 KiB WaiterCostBytes
		// budgets for it.
		name:   "escaped path, id after the last colon",
		prefix: strings.Repeat("A", room/3*3-64) + "%41:",
		source: "a 16 KiB path with one %-escape, retaining 63 KB of heap — over the whole budget once its stack is counted — before the URI was released",
	}, {
		name:   "plain path, id after the last colon",
		prefix: strings.Repeat("A", room-64) + ":",
		source: "the same without the escape: one copy of the line, which the connection pins in lastMethod and no handler can release",
	}, {
		name:   "escaped query",
		query:  "&" + strings.Repeat("%41", (room-64)/3),
		source: "the query half of the same trick, kept in full until the URI was released, since net/http and any logging middleware read the URL",
	}, {
		name:   "half path, half validator",
		prefix: strings.Repeat("A", room/2) + ":",
		block:  "If-None-Match: " + strings.Repeat("e", room/2-100) + "\r\n",
		source: "the two surviving terms at once: they share one wire bound, so spending it twice is not possible",
	}, {
		name:   "fat validator",
		block:  "If-None-Match: " + strings.Repeat("e", room) + "\r\n",
		source: "kept on purpose: dropping it would turn every agent's 304 into a full document",
	}}
}

// parkAndMeasure sends n container lookups in the given shape over n
// connections it keeps open, and returns the heap bytes each parked lookup
// retains.
//
// Each lookup arrives on a REUSED connection, pipelined behind
// keepAlivePrelude in one write. That is not decoration: it is the only shape
// in which the server admits a head as large as maxAdmittedHead, so a harness
// that dialled a fresh connection per request measured a wire term 4 KiB
// smaller than the one a sender can actually spend (see maxAdmittedHead). It is
// also what a real client does — net/http.Transport, which is what every agent
// polls through, keeps its connections.
//
// Retained heap after a GC, not RSS: RSS also carries the allocator's slack and
// the scavenger's schedule, which move between runs by more than the figure
// under test. The store's WaiterCostBytes is rounded up past the RSS of the
// same measurement for exactly that reason.
//
// It RELEASES everything it created before it returns, and JOINS: the parks are
// drained and the goroutine count is back where it started. That is what makes
// two measurements comparable, and its absence is the other half of why the
// previous version of this test could not be run twice. It left each round's
// server and its 200 parked connections to a t.Cleanup, so the next round took
// its baseline snapshot while the previous round's goroutines were still
// unwinding, and their exits then landed inside that round's delta — a
// SUBTRACTION from a figure the round was supposed to be adding to. Nothing in
// that is specific to -count=2; a second iteration only guarantees there is a
// previous round to be polluted by. With the join in place an ordinary poll
// measured between 14,261 and 14,834 B over 120 measurements in twelve
// processes. (retainedWhileParked, the fuzz half, joined its handlers from the
// start and says why in as many words. The rule is the same one; only this half
// was not applying it.)
func parkAndMeasure(t *testing.T, n int, shape hostileShape) (perWaiter int) {
	t.Helper()

	st := store.New(time.Minute)
	st.SetMaxWaiters(n + 10)
	// A wait budget far longer than the test, so nothing unparks underneath the
	// measurement.
	api := newAPI(st, 10*time.Minute)
	srv := api.HTTPServer(":0")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()

	conns := make([]net.Conn, 0, n)
	// The count this measurement has to give back. Taken after the Serve
	// goroutine exists, so the join below is satisfied by <= rather than by an
	// exact match.
	quiescent := runtime.NumGoroutine()
	defer func() {
		// Drain BEFORE closing, because closing is not enough to release a park.
		// A shape carrying Transfer-Encoding has an unread body, so net/http
		// never starts the background read whose EOF cancels the request context
		// (server.go's requestBodyRemains), and the handler would sit in the
		// store for the whole 10-minute wait however hard the client's end is
		// slammed. Server.Drain is the process's own answer to that (503 +
		// Retry-After, the shutdown contract) and it is what makes the join
		// terminate.
		api.Drain()
		for _, c := range conns {
			_ = c.Close()
		}
		_ = srv.Close()
		deadline := time.Now().Add(30 * time.Second)
		for st.BlockedLookups() > 0 || runtime.NumGoroutine() > quiescent {
			if time.Now().After(deadline) {
				// Errorf, not Fatalf: this runs deferred, possibly already on
				// the way out of a failed assertion.
				t.Errorf("this measurement did not give its goroutines back: %d lookups still parked, %d "+
					"goroutines against %d before it ran — the next measurement's baseline would be taken "+
					"while these unwind, which is the noise this harness exists to keep out",
					st.BlockedLookups(), runtime.NumGoroutine(), quiescent)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	dialer := &retryingDialer{t: t, addr: ln.Addr().String(), need: n}
	defer dialer.report()

	baseHeap := retainedHeap()
	for i := range n {
		// A dial that fails is waited out, never skipped on: see retryingDialer
		// for why a sibling test's socket churn must not be able to delete this
		// measurement and leave the run green.
		c := dialer.dial()
		// Tear this connection down with an RST rather than a FIN. The measured
		// figures do not care — the sockets are ESTABLISHED for the whole of the
		// measurement and this only takes effect at Close — but the rest of the
		// package does: 200 connections per measurement and twenty-odd
		// measurements per iteration leave every one of them in TIME_WAIT for a
		// minute, which was 25,000-42,500 sockets against the 28,232-port
		// ephemeral range this machine has, i.e. a dial failure for whatever test
		// runs next. With the RST it is a few hundred.
		if tcp, ok := c.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		conns = append(conns, c)
		// A distinct, well-formed 64-hex container id per request: the store
		// parks per ID, and an over-length id degrades to a non-blocking miss.
		req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: h\r\n%s\r\n",
			shape.target(fmt.Sprintf("%064x", i)), shape.block)
		if len(req) > maxAdmittedHead {
			t.Fatalf("this shape's head is %d bytes, over the %d this server admits behind a %d-byte "+
				"prelude: it would be answered with a 431 and never park, so the measurement would be "+
				"of nothing", len(req), maxAdmittedHead, len(keepAlivePrelude))
		}
		// One write, so the prelude and the request under measurement are in
		// the socket together: that is what leaves part of the second head
		// already in the connection's bufio reader, uncharged against the read
		// limit, which is the whole reason maxAdmittedHead is what it is.
		if _, err := c.Write([]byte(keepAlivePrelude + req)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Every one of them must be visible as a waiter before the snapshot, or the
	// delta below is divided by a count the process never held. A shape that is
	// answered instead of parked (a 431, a 400) never gets here — which is the
	// point: these are all inside the byte bound, so they PARK, and the cost of
	// parking is what is under test.
	deadline := time.Now().Add(30 * time.Second)
	for st.BlockedLookups() < n {
		if time.Now().After(deadline) {
			// The prelude's own 200 comes first on every connection; what
			// matters is whatever follows it, which is empty when the lookup
			// parked and a status line when it did not.
			_ = conns[0].SetReadDeadline(time.Now().Add(time.Second))
			br := bufio.NewReader(conns[0])
			if resp, err := http.ReadResponse(br, nil); err == nil {
				_ = resp.Body.Close()
			}
			var probe [256]byte
			m, _ := br.Read(probe[:])
			t.Fatalf("only %d of %d requests parked; after the prelude's response the first connection "+
				"says %q", st.BlockedLookups(), n, probe[:m])
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The stack term, checked at the snapshot point rather than taken on trust
	// for the rest of the file: parkedStackAllowance is sized for
	// parkedStackGoroutines of them per waiter, and a flat allowance is a number
	// nobody can be wrong about until somebody counts. One read of a counter the
	// runtime already maintains, against the baseline the join at the top of
	// this function already took.
	if got, want := runtime.NumGoroutine()-quiescent, parkedStackGoroutines*n+goroutineSlack; got > want {
		t.Fatalf("%d parked lookups are holding %d goroutines over the %d this process had before they were "+
			"dialled, which is %.2f each against the %d parkedStackAllowance is sized for: the stack half of "+
			"the cost is the half this test ALLOWS FOR instead of measuring (retainedHeap says why), so an "+
			"extra goroutine per waiter adds ~8 KiB to a parked lookup and moves none of the numbers below",
			n, got, quiescent, float64(got)/float64(n), parkedStackGoroutines)
	}

	perWaiter = int((retainedHeap() - baseHeap) / int64(n))

	// The plausibility floor, and the only LOWER bound in this file. Every other
	// assertion here is a ceiling, so a measurement that collapsed toward zero
	// passes all of them green while measuring nothing at all — a runtime whose
	// HeapAlloc reads stale after runtime.GC(), a retainedHeap loop whose two
	// readings agree by coincidence, an allocation moved somewhere this delta
	// does not span. The BlockedLookups() gate above proves the requests PARKED,
	// which is why it is not this check: it says nothing whatever about the
	// number being divided by n here. (Verified by making retainedHeap return a
	// constant: without this line every shape reports 0 B per waiter and the
	// whole test passes.)
	//
	// 8 KiB is not slack, it is the part of the figure that cannot be argued
	// down: net/http gives every connection a 4096-byte bufio.Reader and a
	// 4096-byte bufio.Writer (server.go's serve, newBufioReader /
	// newBufioWriterSize(…, 4<<10)), both alive for as long as the handler
	// parked on that connection is, before one byte is counted for the conn, the
	// response, the Request or the socket. Measured, the smallest reading of any
	// shape over 880 measurements in forty processes was 13,695 B (the cheapest
	// shapes read 13,695-14,353, an ordinary poll 14,213-15,976), so this sits
	// 1.67x under the floor of the distribution — far enough not to flake on a
	// Go release trimming a few hundred bytes, and nowhere near far enough for a
	// measurement that lost the term it reports to slip under it.
	if perWaiter < plausibleParkedHeap {
		t.Fatalf("%d parked lookups retained %d B each, under the %d B a parked connection cannot help "+
			"holding (two 4 KiB bufio buffers): every other assertion in this file is an upper bound, so a "+
			"figure this small is not a cost that improved, it is a measurement that stopped measuring — "+
			"and it would pass every budget check below",
			n, perWaiter, plausibleParkedHeap)
	}
	return perWaiter
}

// plausibleParkedHeap is the floor a park measurement clears before it is
// believed; parkAndMeasure carries the derivation.
const plausibleParkedHeap = 8 << 10

// retainedWhileParked parks n copies of `head` in the real handler, reached
// through the real mux, and reports the heap bytes one of them still retains
// while parked, together with the If-None-Match validator the head carried
// (the one term the bound allows). ok is false when the request does not reach
// the parking handler at all.
//
// No listener: this is the term the SENDER controls, isolated from the
// per-connection floor the end-to-end test measures. The mux is not optional
// though — r.matches, one of the copies of the path, exists only for a request
// routed by one.
func retainedWhileParked(t *testing.T, head string, n int) (perRequest int, validator string, ok bool) {
	t.Helper()
	st := store.New(time.Minute)
	st.SetMaxWaiters(n + 8)
	h := newAPI(st, 10*time.Minute).Handler()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	// Cancel and JOIN: a straggler from one case still unwinding while the next
	// one takes its baseline would land in that case's delta.
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	parse := func() *http.Request {
		r, err := http.ReadRequest(bufio.NewReader(strings.NewReader(head)))
		if err != nil {
			return nil
		}
		// The body wraps the bufio reader this function allocated, which is the
		// harness's memory and not the request's. A parked handler holds
		// net/http's own pooled reader instead, and the end-to-end test measures
		// that.
		r.Body = http.NoBody
		return r.WithContext(ctx)
	}
	serve := func(r *http.Request) (done chan struct{}) {
		done = make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			h.ServeHTTP(&discardWriter{}, r)
		}()
		return done
	}
	parked := func(want int) bool {
		deadline := time.Now().Add(10 * time.Second)
		for st.BlockedLookups() < want {
			if time.Now().After(deadline) {
				return false
			}
			time.Sleep(time.Millisecond)
		}
		return true
	}

	// One pilot request decides whether this head parks at all — through the
	// real handler, so the test does not restate its admission rules. The
	// decision is taken on POSITIVE evidence in both directions (it parked, or
	// the handler returned), never by waiting out a timeout: this runs under a
	// fuzzer, most of whose inputs do not park, and a one-second skip is a
	// worker the fuzzing harness declares hung.
	pilot := parse()
	if pilot == nil {
		return 0, "", false
	}
	validator = pilot.Header.Get("If-None-Match")
	answered := serve(pilot)
	spin := time.Now().Add(30 * time.Second)
	for st.BlockedLookups() == 0 {
		select {
		case <-answered:
			return 0, "", false // answered instead of parking
		default:
		}
		if time.Now().After(spin) {
			t.Fatal("the pilot request neither parked nor returned: a container lookup has exactly " +
				"two outcomes here, so a third one is a handler that can hold a connection with nothing " +
				"bounding it")
		}
		runtime.Gosched()
	}

	baseHeap := retainedHeap()
	for range n {
		r := parse()
		if r == nil {
			return 0, "", false
		}
		_ = serve(r)
	}
	if !parked(n + 1) {
		t.Fatalf("only %d of %d requests parked after the pilot did", st.BlockedLookups()-1, n)
	}
	// NOT max(delta, 0): clamping a negative delta to zero converts a broken
	// measurement into a passing one, and every other assertion here is an
	// upper bound, so zero passes them all. A park that retains nothing is not
	// a cost that improved — it is a reading that stopped reading.
	perRequest = int((retainedHeap() - baseHeap) / int64(n))
	if perRequest < plausibleRequestHeap {
		t.Fatalf("%d parked requests retained %d B each, under the %d B a parked request "+
			"cannot help holding (its Request, URL, context and response writer): this is a "+
			"measurement that stopped measuring, and it would pass the upper bound above",
			n, perRequest, plausibleRequestHeap)
	}
	return perRequest, validator, true
}

// plausibleRequestHeap is the floor a no-listener park measurement clears
// before it is believed. The connection floor (plausibleParkedHeap, two 4 KiB
// bufio buffers) does not apply here — this measurement deliberately has no
// listener — but a parked request is still a Request, a URL, a context chain
// and a response writer, measured at 1.2-2.3 KB across every shape tried. 512
// is under half the smallest of those: comfortably clear of a real reading,
// and nowhere near a delta that has collapsed to zero or gone negative.
const plausibleRequestHeap = 512

// discardWriter is a per-request http.ResponseWriter that keeps nothing.
type discardWriter struct{ h http.Header }

func (w *discardWriter) Header() http.Header {
	if w.h == nil {
		w.h = http.Header{}
	}
	return w.h
}
func (w *discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *discardWriter) WriteHeader(int)             {}

// retainedHeap reports heap-in-use once collecting stops changing it: the first
// GC frees the previous phase's garbage, the second collects what the first
// made unreachable, and the loop runs until two readings agree rather than
// stopping at a count someone picked. A fixed two was enough for the shapes
// here and is not enough to state as a property — the number of cycles it takes
// to settle is the runtime's business, and this is measurement code whose whole
// value is that its noise is smaller than what it asserts.
//
// Goroutine STACKS are deliberately not part of the figure, and they used to
// be. runtime.MemStats.StackInuse cannot be attributed to a park round: it
// counts stack SPANS, and the runtime pools freed stacks, so whether 200 new
// goroutines show up in it at all depends on whether the previous round's
// stacks were still in the pool when the baseline was taken — a span already
// counted at the baseline serves them for free. The same 200-waiter poll,
// measured ten times in one process, reported between 1,146 and 10,485 B of
// stack per waiter while its heap figure moved by 532 B. An 8 KB swing in a
// term with 8 KiB of tolerance is not a measurement.
//
// So the heap is MEASURED and the stack is ALLOWED FOR, at
// parkedStackAllowance, which is above every honest reading of it (8.2-13.3 KB
// per waiter: one net/http conn goroutine grown to 8 KiB, its background-read
// goroutine, and the span overhead of both). Overstating it can only make the
// budget assertions stricter. Nothing is lost for the property under test
// either: releaseParkedHead governs what a request RETAINS, which is heap, and
// the stack is one conn goroutine whatever the sender sent.
//
// What the allowance takes on trust is not the SIZE of those stacks, which is
// what is unmeasurable here, but their NUMBER, which is not: parkAndMeasure
// asserts it at the snapshot point, and parkedStackGoroutines says why a term
// that is allowed for still has to be checked somewhere.
func retainedHeap() int64 {
	var ms runtime.MemStats
	var prev int64
	for i := range 8 {
		runtime.GC()
		runtime.ReadMemStats(&ms)
		h := int64(ms.HeapAlloc)
		if i > 0 && h == prev {
			return h
		}
		prev = h
	}
	return prev
}

// widestHeaderBlock builds the largest number of DISTINCT minimal header lines
// that fits in budget bytes — the shape that turns a byte bound into a cost
// twenty times larger. Keys are canonicalised case-insensitively by net/textproto,
// so the alphabet is lowercase and distinctness comes from length.
func widestHeaderBlock(budget int) string {
	var sb strings.Builder
	eachKey(budget, 3, func(key string) bool {
		if sb.Len()+len(key)+3 > budget {
			return false
		}
		sb.WriteString(key)
		sb.WriteString(":\r\n")
		return true
	})
	return sb.String()
}

// commaKeyList builds a comma-separated list of distinct keys that fits in
// budget bytes: one header LINE that net/http expands into one map entry each.
func commaKeyList(budget int) string {
	var sb strings.Builder
	eachKey(budget, 3, func(key string) bool {
		if sb.Len()+len(key)+1 > budget {
			return false
		}
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(key)
		return true
	})
	return sb.String()
}

// eachKey feeds distinct lowercase keys of up to maxWidth characters to fn until
// it returns false.
func eachKey(budget, maxWidth int, fn func(string) bool) {
	const alpha = "abcdefghijklmnopqrstuvwxyz0123456789"
	key := make([]byte, 0, maxWidth)
	for width := 1; width <= maxWidth; width++ {
		n := 1
		for range width {
			n *= len(alpha)
		}
		for i := range n {
			key = key[:0]
			for d, v := 0, i; d < width; d, v = d+1, v/len(alpha) {
				key = append(key, alpha[v%len(alpha)])
			}
			if !fn(string(key)) {
				return
			}
		}
	}
}
