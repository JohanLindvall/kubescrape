package otlpexport

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/spool"
)

// audit_test.go: targeted tests from the 2026-07 audit.

// failSender always rejects with a fixed error and counts the attempts.
type failSender struct {
	err      error
	attempts atomic.Int64
	mu       sync.Mutex
	ok       []string
}

func (f *failSender) ExportLogs(_ context.Context, _ plog.Logs) error {
	f.attempts.Add(1)
	return f.err
}

func (f *failSender) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	f.attempts.Add(1)
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ok = append(f.ok, md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
	return nil
}

// IsPermanent classification against the statuses/codes a real collector
// returns. Pins the deliberate choices (auth + 404 transient) and the ones that
// matter for the drain's liveness.
func TestIsPermanentClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"http 400 bad payload", &HTTPStatusError{Code: 400}, true},
		{"http 413 too large", &HTTPStatusError{Code: 413}, true},
		{"http 415 media type", &HTTPStatusError{Code: 415}, true},
		{"http 401 rotating token", &HTTPStatusError{Code: 401}, false},
		{"http 403", &HTTPStatusError{Code: 403}, false},
		{"http 404 collector rollout", &HTTPStatusError{Code: http.StatusNotFound}, false},
		{"http 429 throttled", &HTTPStatusError{Code: 429}, false},
		{"http 502", &HTTPStatusError{Code: 502}, false},
		{"http 503", &HTTPStatusError{Code: 503}, false},
		// 501: the OTLP/HTTP twin of gRPC Unimplemented (the signal's pipeline
		// is not configured). Classified transient while the gRPC form is
		// permanent — see TestPoisonBatchIsDroppedOnceCollectorTakesOthers.
		{"http 501 not implemented", &HTTPStatusError{Code: 501}, false},
		{"grpc InvalidArgument", status.Error(codes.InvalidArgument, "bad"), true},
		{"grpc Unimplemented", status.Error(codes.Unimplemented, "no logs pipeline"), true},
		{"grpc Unavailable", status.Error(codes.Unavailable, "down"), false},
		{"grpc ResourceExhausted", status.Error(codes.ResourceExhausted, "message larger than max"), false},
		{"grpc Unauthenticated", status.Error(codes.Unauthenticated, "token"), false},
		{"grpc DeadlineExceeded", status.Error(codes.DeadlineExceeded, "timeout"), false},
		{"plain error", errors.New("dial tcp: connection refused"), false},
		{"spool full", spool.ErrFull, false},
	}
	for _, tc := range cases {
		if got := IsPermanent(tc.err); got != tc.want {
			t.Errorf("%s: IsPermanent = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A batch the collector will never accept, whose rejection is (correctly)
// classified transient — the classic case is a payload above the collector's
// gRPC recv limit, returned as codes.ResourceExhausted, retryable per the OTLP
// failure table — must not circle the spool forever. It is dropped and counted
// once it has failed repeatedly WHILE THE COLLECTOR IS ACCEPTING OTHER BATCHES,
// which is what distinguishes a poison payload from an outage.
func TestPoisonBatchIsDroppedOnceCollectorTakesOthers(t *testing.T) {
	send := &selectiveSender{reject: "huge", err: status.Error(codes.ResourceExhausted, "grpc: received message larger than max")}
	dir := t.TempDir()
	ls, err := spool.Open(dir+"/logs", spool.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	ms, err := spool.Open(dir+"/metrics", spool.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ms.Close() }()
	b := NewBuffered(send, ls, ms, time.Millisecond, nil)

	if err := b.ExportMetrics(context.Background(), metricsWith("huge")); err != nil {
		t.Fatal(err)
	}
	// Good data behind it: the collector takes these, proving it is alive.
	for i := 0; i < 3; i++ {
		if err := b.ExportMetrics(context.Background(), metricsWith("good")); err != nil {
			t.Fatal(err)
		}
	}
	before := obs.BufferDropped.WithLabelValues("metrics").Value()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	waitFor(t, func() bool {
		return obs.BufferDropped.WithLabelValues("metrics").Value()-before == 1
	}, "poison batch dropped and counted")
	waitFor(t, func() bool { return ms.Bytes() == 0 }, "spool fully drained")
	if got := send.delivered(); got < 3 {
		t.Errorf("delivered %d good batches, want the 3 queued behind the poison one", got)
	}
}

// The counterpart, and the reason the drop is conditional: during a collector
// OUTAGE every batch fails exactly like a poison one. Dropping then would
// breach the zero-loss guarantee for logs, so nothing may be given up on while
// nothing at all is getting through — the data waits for the collector.
func TestOutageNeverDropsBufferedData(t *testing.T) {
	send := &failSender{err: status.Error(codes.Unavailable, "collector down")}
	dir := t.TempDir()
	ls, err := spool.Open(dir+"/logs", spool.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	ms, err := spool.Open(dir+"/metrics", spool.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ms.Close() }()
	b := NewBuffered(send, ls, ms, time.Millisecond, nil)

	for i := 0; i < 3; i++ {
		if err := b.ExportMetrics(context.Background(), metricsWith("data")); err != nil {
			t.Fatal(err)
		}
	}
	before := obs.BufferDropped.WithLabelValues("metrics").Value()
	queued := ms.Bytes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	// Past the per-batch budget (maxDrainCycles cycles x stuckAfterAttempts
	// sends): if an outage could exhaust it, the data would be gone by now.
	const past = maxDrainCycles*stuckAfterAttempts + 1
	deadline := time.Now().Add(15 * time.Second)
	for send.attempts.Load() < past && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := send.attempts.Load(); got < past {
		t.Fatalf("only %d attempts; test did not exercise the retry loop past the budget (%d)", got, past)
	}
	if got := obs.BufferDropped.WithLabelValues("metrics").Value() - before; got != 0 {
		t.Errorf("dropped %v batches during an outage; buffered data must never be given up on "+
			"while the collector is accepting nothing", got)
	}
	if ms.Bytes() < queued {
		t.Errorf("spool shrank from %d to %d bytes during an outage: data was discarded", queued, ms.Bytes())
	}
}

// selectiveSender accepts every batch except the one metric it is told to
// reject — a live collector refusing one poison payload.
type selectiveSender struct {
	reject string
	err    error
	mu     sync.Mutex
	ok     []string
}

func (s *selectiveSender) ExportLogs(_ context.Context, _ plog.Logs) error { return nil }

func (s *selectiveSender) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	name := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name()
	if name == s.reject {
		return s.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ok = append(s.ok, name)
	return nil
}

func (s *selectiveSender) delivered() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ok)
}

// A permanently-rejected batch must not block the batches behind it (pinned:
// this is the path that DOES terminate).
func TestPermanentRejectionDoesNotBlockQueue(t *testing.T) {
	send := &fakeSender{}
	b, ls, ms := openBuffer(t, t.TempDir(), send, 0)
	defer func() { _ = ls.Close() }()
	defer func() { _ = ms.Close() }()
	_ = b
	if err := ms.Append([]byte("not protobuf")); err != nil {
		t.Fatal(err)
	}
	if err := b.ExportMetrics(context.Background(), metricsWith("cpu")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)
	waitFor(t, func() bool { return len(send.gotMetrics()) == 1 }, "the batch behind the poison frame")
}
