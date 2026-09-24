package otlpexport

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/backoff"
)

// Retry with attempts <= 0 must still send exactly once: returning nil without
// calling send would report a success for a send that never happened.
func TestRetryAttemptsBelowOneStillSendsOnce(t *testing.T) {
	for _, attempts := range []int{0, -1} {
		calls := 0
		err := Retry(context.Background(), attempts, time.Millisecond, func() error {
			calls++
			return nil
		})
		if err != nil {
			t.Fatalf("attempts=%d: unexpected error %v", attempts, err)
		}
		if calls != 1 {
			t.Fatalf("attempts=%d: send called %d times, want 1", attempts, calls)
		}
	}
}

// Retry's doubling stops at backoff.Cap: attempts is the operator's
// -otlp-retry-attempts, which has no upper bound, and an uncapped doubling
// made the tenth wait 256s.
func TestRetryBackoffNeverWaitsPastTheCap(t *testing.T) {
	var waits []time.Duration
	done, cancel := context.WithCancel(context.Background())
	cancel()
	orig := retryWait
	retryWait = func(b *backoff.B, _ context.Context) bool {
		waits = append(waits, b.Delay())
		b.Wait(done) // the real doubling, without the real wait
		return true
	}
	defer func() { retryWait = orig }()

	const attempts = 12
	calls := 0
	_ = Retry(context.Background(), attempts, time.Second, func() error {
		calls++
		return errors.New("collector down")
	})
	if calls != attempts {
		t.Fatalf("send called %d times, want %d", calls, attempts)
	}
	if len(waits) != attempts-1 {
		t.Fatalf("waited %d times, want %d (never after the final failure)", len(waits), attempts-1)
	}
	if waits[0] != time.Second || waits[1] != 2*time.Second {
		t.Fatalf("waits start %v, want the doubling from the initial 1s", waits[:2])
	}
	for i, w := range waits {
		if w > backoff.Cap {
			t.Fatalf("wait %d is %v, past the %v cap: %v", i, w, backoff.Cap, waits)
		}
	}
	if last := waits[len(waits)-1]; last != backoff.Cap {
		t.Fatalf("last wait %v, want the doubling to have reached the %v cap", last, backoff.Cap)
	}

	// An -otlp-retry-backoff ABOVE the cap is clamped too, the first wait
	// included, and the waits never shrink: it used to wait 1m, then 30s, 30s.
	waits = nil
	_ = Retry(context.Background(), 4, time.Minute, func() error { return errors.New("collector down") })
	for i, w := range waits {
		if w > backoff.Cap {
			t.Fatalf("with a 1m initial backoff, wait %d is %v, past the %v cap: %v", i, w, backoff.Cap, waits)
		}
		if i > 0 && w < waits[i-1] {
			t.Fatalf("with a 1m initial backoff, wait %d (%v) is shorter than wait %d: %v", i, w, i-1, waits)
		}
	}
}

// The disk-buffer drain's per-cycle floor is the same flag, and it is clamped
// the same way: a 1m -otlp-retry-backoff made every drain cycle wait 1m, then
// 30s, and climb back to 1m at the next cycle's floor.
func TestDrainBackoffFloorNeverExceedsTheCap(t *testing.T) {
	s := &sink[plog.Logs]{
		kind:    "logs",
		log:     quietLogger(),
		backoff: time.Minute,
		send:    func(context.Context, plog.Logs) error { return context.DeadlineExceeded },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the first wait returns at once; what matters is its length
	ld := plog.NewLogs()
	if got := s.trySend(ctx, func(c context.Context) error { return s.send(c, ld) }, false); got != sendCancelled {
		t.Fatalf("trySend = %v, want sendCancelled", got)
	}
	if s.cur > backoff.Cap {
		t.Fatalf("the drain waited %v with a 1m -otlp-retry-backoff, past the %v cap", s.cur, backoff.Cap)
	}
}
