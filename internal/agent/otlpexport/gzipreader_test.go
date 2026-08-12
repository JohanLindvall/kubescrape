package otlpexport

// The gRPC gzip codec pools its READERS as well as its writers. A gzip reader
// is 36.7 kB and 5 allocations (a fixed 32 KiB LZ77 window plus the huffman
// tables), measured, and one was minted per RECEIVED message on both halves of
// every gRPC hop — the ingest receiver's pushes and, because grpc-go compresses responses
// too, the two-byte ExportResponse that answers every export.
//
// Pooling a reader is only safe while nothing can still read from one that has
// gone back, so the tests here pin the release rules rather than the saving:
// released exactly once, never under a live reader, and carrying none of the
// last message's sender-controlled header into the pool.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// requirePoolIdentity guards the assertions that follow one reader from a Put
// to the next Get. The race detector makes that unobservable on purpose:
// sync.Pool DELIBERATELY drops one Put in four when built with -race, to shake
// out exactly the assumption these tests make deliberately. They run in the
// ordinary build, which is what `go test ./...` is.
func requirePoolIdentity(t *testing.T) {
	t.Helper()
	if testrace.Enabled {
		t.Skip("sync.Pool drops one Put in four under the race detector; reader identity across the pool is not observable")
	}
	withSingleP(t)
}

// withSingleP makes the codec's sync.Pool deterministic for the reader-identity
// assertions below: Put fills the CURRENT P's private slot and Get reads the
// current P's (a private slot cannot be stolen), so with several Ps a test
// goroutine that migrates between the two would miss the pooled reader and fail
// for a scheduling reason rather than a pooling one.
func withSingleP(t *testing.T) {
	t.Helper()
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
}

func gzipTestBytes(tb testing.TB, body []byte, hdr func(*gzip.Writer)) []byte {
	tb.Helper()
	var out bytes.Buffer
	z := gzip.NewWriter(&out)
	if hdr != nil {
		hdr(z)
	}
	if _, err := z.Write(body); err != nil {
		tb.Fatal(err)
	}
	if err := z.Close(); err != nil {
		tb.Fatal(err)
	}
	return out.Bytes()
}

func decompressTest(tb testing.TB, c *gzipCodec, gz []byte) (*pooledGzipReader, *gzip.Reader) {
	tb.Helper()
	r, err := c.Decompress(bytes.NewReader(gz))
	if err != nil {
		tb.Fatalf("Decompress: %v", err)
	}
	p, ok := r.(*pooledGzipReader)
	if !ok {
		tb.Fatalf("Decompress returned %T, not a pooled reader", r)
	}
	return p, p.z
}

// The saving itself: a reader that has ended goes back and the next message
// reuses it, window and all.
func TestGzipReaderIsPooledAndReused(t *testing.T) {
	requirePoolIdentity(t)
	c := &gzipCodec{}
	gz := gzipTestBytes(t, []byte("hello, collector"), nil)

	p1, z1 := decompressTest(t, c, gz)
	got, err := io.ReadAll(p1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello, collector" {
		t.Fatalf("decompressed %q", got)
	}
	if p1.z != nil {
		t.Fatal("a reader read to EOF was not released")
	}

	_, z2 := decompressTest(t, c, gz)
	if z2 != z1 {
		t.Fatal("the next message allocated a fresh gzip reader: 36.7 kB per received message, which is what the pool exists to stop")
	}
}

// grpc-go Closes the reader (`defer closer.Close()` in its decompress) on a
// reader a terminal Read has usually already released. A second Put would hand
// ONE reader to two consumers, whose messages would then interleave through a
// single 32 KiB window — corruption, not a leak.
func TestGzipReaderIsReleasedExactlyOnce(t *testing.T) {
	requirePoolIdentity(t)
	c := &gzipCodec{}
	gz := gzipTestBytes(t, []byte("released once"), nil)

	p, z := decompressTest(t, c, gz)
	if _, err := io.ReadAll(p); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := p.Close(); err != nil { // the Close grpc-go always makes
		t.Fatalf("Close: %v", err)
	}

	// Two readers held at the SAME time: if the double release pooled one
	// reader twice, these are the same object.
	_, za := decompressTest(t, c, gz)
	_, zb := decompressTest(t, c, gz)
	if za == zb {
		t.Fatal("two live readers share one *gzip.Reader: the reader was pooled twice, and two concurrent messages would decode through one window")
	}
	if za != z && zb != z {
		t.Fatal("the released reader never came back from the pool")
	}
}

// grpc-go stops reading SHORT of EOF whenever a message exceeds the receive
// limit (it reads through a LimitReader of max+1) — precisely the payloads a
// hostile sender picks. Releasing on EOF alone would send those readers to the
// GC, so the Close gate is what covers them.
func TestGzipReaderClosedShortOfEOFIsPooled(t *testing.T) {
	requirePoolIdentity(t)
	c := &gzipCodec{}
	gz := gzipTestBytes(t, bytes.Repeat([]byte("payload over the limit. "), 4000), nil)

	p, z := decompressTest(t, c, gz)
	var head [8]byte
	if _, err := p.Read(head[:]); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if p.z != nil {
		t.Fatal("Close did not release a reader that had not reached EOF")
	}
	if _, z2 := decompressTest(t, c, gz); z2 != z {
		t.Fatal("a reader closed short of EOF was not pooled: every over-limit message would allocate 36.7 kB and drop it")
	}
}

// A handle whose reader has gone back to the pool must answer from its own
// recorded end. Reading through to the pooled reader would consume bytes that
// now belong to whoever holds it.
func TestGzipReadAfterReleaseDoesNotTouchThePooledReader(t *testing.T) {
	requirePoolIdentity(t)
	c := &gzipCodec{}

	stale, z := decompressTest(t, c, gzipTestBytes(t, []byte("first message"), nil))
	if _, err := io.ReadAll(stale); err != nil {
		t.Fatalf("read: %v", err)
	}

	next, z2 := decompressTest(t, c, gzipTestBytes(t, []byte("second message"), nil))
	if z2 != z {
		t.Fatal("the pool did not hand the same reader back: without the reuse there is nothing for a stale handle to steal from, and this test would prove nothing")
	}

	var b [64]byte
	n, err := stale.Read(b[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("a late Read on a released handle returned (%d, %v), want (0, EOF)", n, err)
	}
	got, err := io.ReadAll(next)
	if err != nil {
		t.Fatalf("read of the reader's new owner: %v", err)
	}
	if string(got) != "second message" {
		t.Fatalf("the stale handle stole bytes from the reader's new owner: got %q", got)
	}
}

// A message closed before it ended must not later read as a clean EOF: a
// truncated payload reported as a whole one decodes as a whole one.
func TestGzipReadAfterCloseIsNotReportedAsEOF(t *testing.T) {
	c := &gzipCodec{}
	p, _ := decompressTest(t, c, gzipTestBytes(t, bytes.Repeat([]byte("x"), 100000), nil))
	var head [8]byte
	if _, err := p.Read(head[:]); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := p.Read(head[:]); !errors.Is(err, errGzipReadAfterClose) {
		t.Fatalf("a Read after an early Close returned %v; io.EOF here would report a truncated message as a complete one", err)
	}
}

// The gzip header is SENDER-CONTROLLED — FEXTRA up to 64 KiB, FNAME and
// FCOMMENT up to 512 bytes each — and it is allocated per message. A reader
// pooled with its header intact parks that memory for as long as the pool
// holds it, on listeners that are unauthenticated by design.
func TestPooledGzipReaderHoldsNoSenderMemory(t *testing.T) {
	c := &gzipCodec{}
	extra := bytes.Repeat([]byte{7}, 8192)
	gz := gzipTestBytes(t, []byte("body"), func(z *gzip.Writer) {
		z.Name = "sender-chosen-name"
		z.Comment = "sender-chosen-comment"
		z.Extra = extra
	})

	p, z := decompressTest(t, c, gz)
	if len(z.Extra) != len(extra) || z.Name == "" || z.Comment == "" {
		t.Fatalf("the header under test was not parsed (extra=%d name=%q); the assertion below would pass vacuously", len(z.Extra), z.Name)
	}
	if _, err := io.ReadAll(p); err != nil {
		t.Fatalf("read: %v", err)
	}
	if z.Extra != nil || z.Name != "" || z.Comment != "" {
		t.Fatalf("a pooled reader still holds the sender's header: %d extra bytes, name %q, comment %q", len(z.Extra), z.Name, z.Comment)
	}
}

// The idle stream is hard-coded, so the parser it has to satisfy is what pins
// it. putGzipReader treats a failed Reset as unreachable and DROPS the reader;
// a wrong byte here would quietly turn pooling off for the whole process.
func TestIdleGzipStreamParses(t *testing.T) {
	zr, err := gzip.NewReader(bytes.NewReader(idleGzipStream[:]))
	if err != nil {
		t.Fatalf("the idle stream is not a gzip stream: %v", err)
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(b) != 0 {
		t.Fatalf("the idle stream decodes to %d bytes, want 0", len(b))
	}
	// The scrub only ever runs readHeader, which is the part that must not need
	// optional fields: FLG must be zero.
	if flg := idleGzipStream[3]; flg != 0 {
		t.Fatalf("FLG = %#x, want 0 (any flag makes readHeader read sender-shaped optional fields)", flg)
	}
}

// The OTHER thing a pooled reader must not park, and the one a Reset onto an
// empty source does NOT release: the message's own source. gzip re-points its
// flate decompressor at the last statement of readHeader, so a Reset whose
// header read fails leaves the decompressor — and any bufio.Reader gzip had to
// wrap the source in — pointing at the message that just finished, for as long
// as the pool holds the reader.
//
// This measures the RETENTION and not a byte count, deliberately: how much a
// retained source holds is the source's business, and on the gRPC path it is a
// drained reader holding nothing (TestGRPCSourceIsDrainedWhenTheReaderIsPooled
// measures that separately). What the release buys is that the pooled reader
// keeps no pointer into the last message whatever the source turns out to be.
//
// Both source shapes are here because they are retained through different
// fields and only one of them is reachable from a ByteReader idle source.
func TestPooledGzipReaderReleasesTheMessagesSource(t *testing.T) {
	requirePoolIdentity(t)
	for _, byteReader := range []bool{true, false} {
		name := "plain source"
		if byteReader {
			name = "ByteReader source"
		}
		t.Run(name, func(t *testing.T) {
			c := &gzipCodec{}
			var collected atomic.Bool
			// In a function of its own so no live stack slot in the test frame
			// can keep the source reachable on its own.
			func() {
				gz := gzipTestBytes(t, []byte("a message"), nil)
				var src io.Reader = &plainSource{r: bytes.NewReader(gz)}
				if byteReader {
					src = &byteSource{plainSource{r: bytes.NewReader(gz)}}
				}
				runtime.SetFinalizer(src, func(any) { collected.Store(true) })
				r, err := c.Decompress(src)
				if err != nil {
					t.Fatalf("Decompress: %v", err)
				}
				got, err := io.ReadAll(r) // reads to EOF: the reader is pooled here
				if err != nil || string(got) != "a message" {
					t.Fatalf("read %q: %v", got, err)
				}
			}()
			// Taking the reader back out is what makes the measurement mean
			// something: the source must be unreachable while the reader that
			// read it is still alive.
			z, ok := c.readers.Get().(*gzip.Reader)
			if !ok {
				t.Fatal("the reader never reached the pool; this test would prove nothing")
			}
			// A finalizer runs on its own goroutine after the collection that
			// queued it, so the wait is for the SCHEDULER, not for the GC. A
			// source that is still referenced is never collected however long
			// this loop runs, so the retries cost nothing but a failing test's
			// patience.
			for i := 0; i < 50 && !collected.Load(); i++ {
				runtime.GC()
				time.Sleep(time.Millisecond)
			}
			// The pooled reader is held ALIVE for the whole check: without this
			// the source could be collected because the READER was, which is
			// not what is being measured.
			runtime.KeepAlive(z)
			if !collected.Load() {
				t.Fatal("a pooled reader still references the message's source, and it holds that reference for as long as the pool holds the reader")
			}
		})
	}
}

// What the retained reference is actually worth on the path this codec serves,
// stated as a measurement because the comment on putGzipReader used to state it
// as "up to MaxRecvMsgSize per pooled reader" and that was not measured — and
// is not true. grpc-go hands the codec a *mem.Reader over the message's buffers
// and frees each buffer as it is read, so by the time the gzip reader is pooled
// its source is drained: a reference, not a message.
//
// This pins a DEPENDENCY's behaviour on purpose. If grpc-go stops freeing as it
// reads, the release in putGzipReader starts holding real bytes and this test
// is the alarm that says so — which is the point of writing the claim down as
// an assertion rather than as prose.
func TestGRPCSourceIsDrainedWhenTheReaderIsPooled(t *testing.T) {
	c := &gzipCodec{}
	gz := gzipTestBytes(t, bytes.Repeat([]byte("a message worth compressing "), 100), nil)
	// Exactly what rpc_util.go's decompress passes: BufferSlice.Reader() over
	// the received message.
	src := mem.BufferSlice{mem.SliceBuffer(gz)}.Reader()
	if got := src.Remaining(); got != len(gz) {
		t.Fatalf("the source holds %d of %d compressed bytes before the read; the assertion below would pass vacuously", got, len(gz))
	}

	r, err := c.Decompress(src)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if _, err := io.ReadAll(r); err != nil { // reads to EOF: the reader is pooled here
		t.Fatalf("read: %v", err)
	}
	if got := src.Remaining(); got != 0 {
		t.Fatalf("grpc-go's message reader still holds %d compressed bytes at the moment the gzip reader is pooled: the exposure putGzipReader's Reset removes is bytes after all, and its comment says it is only a reference", got)
	}
}

// plainSource is an io.Reader and nothing more: gzip has to wrap it in a
// bufio.Reader, which is the field the retained reference hides in.
type plainSource struct{ r *bytes.Reader }

func (s *plainSource) Read(p []byte) (int, error) { return s.r.Read(p) }

// byteSource is what grpc-go hands the codec: a reader gzip stores directly.
type byteSource struct{ plainSource }

func (s *byteSource) ReadByte() (byte, error) { return s.r.ReadByte() }

// A malformed header is what an unauthenticated sender produces at will. If
// that path dropped the reader instead of pooling it, a stream of junk would
// turn pooling off for the whole process.
func TestMalformedGzipHeaderKeepsTheReaderInThePool(t *testing.T) {
	requirePoolIdentity(t)
	c := &gzipCodec{}
	gz := gzipTestBytes(t, []byte("good"), nil)

	p, z := decompressTest(t, c, gz)
	if _, err := io.ReadAll(p); err != nil {
		t.Fatalf("read: %v", err)
	}
	if _, err := c.Decompress(bytes.NewReader([]byte("definitely not a gzip stream"))); err == nil {
		t.Fatal("a malformed header must be an error")
	}
	if _, z2 := decompressTest(t, c, gz); z2 != z {
		t.Fatal("a malformed header leaked the reader out of the pool")
	}
}

// Close may come from a different goroutine than the reads. Run with -race:
// without the mutex the detector reports the concurrent access, and the reader
// could go back to the pool underneath an in-flight Read.
func TestGzipReaderCloseIsSafeAgainstAnInFlightRead(t *testing.T) {
	c := &gzipCodec{}
	gz := gzipTestBytes(t, bytes.Repeat([]byte("concurrent close. "), 200000), nil)

	for i := 0; i < 20; i++ {
		p, _ := decompressTest(t, c, gz)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4096)
			for {
				if _, err := p.Read(buf); err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			_ = p.Close()
		}()
		wg.Wait()
		if p.z != nil {
			t.Fatal("the reader was not released")
		}
	}
}

// The budget the pooling exists for, ENFORCED rather than only reported by the
// benchmark: one allocation, the per-message wrapper. Unpooled — Decompress as
// it was, `return gzip.NewReader(r)` — this same function measures 17: the
// reader's own five (one of them the 32 KiB window, and the figure the 2-byte
// response benchmark isolates), plus twelve a fresh decompressor makes while
// reading a 460 KB body.
func TestGzipDecompressAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	c := &gzipCodec{}
	gz := gzipTestBytes(t, benchBody(), nil)
	src := bytes.NewReader(nil)
	sink := make([]byte, 32*1024)

	got := testing.AllocsPerRun(20, func() {
		src.Reset(gz)
		r, err := c.Decompress(src)
		if err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := r.Read(sink); err != nil {
				break
			}
		}
		_ = r.(io.Closer).Close()
	})
	if want := 2.0; got > want {
		t.Errorf("decompressing a message allocates %.0f times, want <= %.0f (unpooled, measured, this is 17)", got, want)
	}
}

// End-to-end over real gRPC with gzip: many concurrent exports whose bodies the
// server verifies byte for byte. A reader shared by two in-flight messages
// yields a corrupt protobuf or the wrong records, which surfaces here as a
// failed export rather than as a subtle accounting difference.
func TestGRPCGzipConcurrentRoundTripIsIntact(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sink := &verifyingLogSink{}
	srv := grpc.NewServer()
	plogotlp.RegisterGRPCServer(srv, sink)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	c, err := New(Config{
		Endpoint: lis.Addr().String(), Protocol: "grpc", Insecure: true,
		Compression: "gzip", Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	const senders, rounds = 8, 25
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			ld := verifiableLogs(fmt.Sprintf("sender-%d", s))
			for r := 0; r < rounds; r++ {
				if err := c.ExportLogs(context.Background(), ld); err != nil {
					t.Errorf("export: %v", err)
					return
				}
			}
		}(s)
	}
	wg.Wait()
	if got, want := sink.records.Load(), int64(senders*rounds*verifiableRecords); got != want {
		t.Errorf("collector received %d records, want %d", got, want)
	}
}

const verifiableRecords = 200

// verifiableLogs builds a payload every record of which names its sender and
// its index, so the server can prove the decompressed bytes are the ones this
// sender wrote.
func verifiableLogs(tag string) plog.Logs {
	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := 0; i < verifiableRecords; i++ {
		lrs.AppendEmpty().Body().SetStr(fmt.Sprintf("%s/%04d/%s", tag, i, "padding to make the payload worth compressing "))
	}
	return ld
}

type verifyingLogSink struct {
	plogotlp.UnimplementedGRPCServer
	records atomic.Int64
}

func (s *verifyingLogSink) Export(_ context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	recs := req.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if recs.Len() != verifiableRecords {
		return plogotlp.NewExportResponse(), fmt.Errorf("got %d records, want %d", recs.Len(), verifiableRecords)
	}
	tag, _, _ := bytes.Cut([]byte(recs.At(0).Body().Str()), []byte("/"))
	for i := 0; i < recs.Len(); i++ {
		if want := fmt.Sprintf("%s/%04d/%s", tag, i, "padding to make the payload worth compressing "); recs.At(i).Body().Str() != want {
			return plogotlp.NewExportResponse(), fmt.Errorf("record %d is %q, want %q", i, recs.At(i).Body().Str(), want)
		}
	}
	s.records.Add(int64(recs.Len()))
	return plogotlp.NewExportResponse(), nil
}
