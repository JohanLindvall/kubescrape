package backoff

import (
	"context"
	"testing"
	"time"
)

// cancelled lets Sleep return immediately so the delay arithmetic is testable
// without sleeping.
func cancelled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestSleepDoublesToCapAndResets(t *testing.T) {
	ctx := cancelled()
	b := New(4 * time.Second)
	if got := b.Delay(); got != 4*time.Second {
		t.Fatalf("initial Delay = %v, want 4s", got)
	}
	b.Sleep(ctx)
	if got := b.Delay(); got != 8*time.Second {
		t.Fatalf("after one Sleep Delay = %v, want 8s", got)
	}
	for range 10 {
		b.Sleep(ctx)
	}
	if got := b.Delay(); got != Cap {
		t.Fatalf("Delay = %v, want the %v cap", got, Cap)
	}
	b.Reset()
	if got := b.Delay(); got != 4*time.Second {
		t.Fatalf("after Reset Delay = %v, want 4s", got)
	}
}

func TestResetIfHealthyUsesTheCapAsThreshold(t *testing.T) {
	ctx := cancelled()
	b := New(time.Second)
	b.Sleep(ctx)
	// A run shorter than Cap is not proof of health.
	b.ResetIfHealthy(time.Now())
	if got := b.Delay(); got != 2*time.Second {
		t.Fatalf("short run reset the backoff: Delay = %v, want 2s", got)
	}
	// A run at least Cap long is.
	b.ResetIfHealthy(time.Now().Add(-Cap - time.Second))
	if got := b.Delay(); got != time.Second {
		t.Fatalf("healthy run did not reset: Delay = %v, want 1s", got)
	}
}

func TestSleepHonorsContext(t *testing.T) {
	b := New(time.Minute)
	start := time.Now()
	b.Sleep(cancelled())
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Sleep on a cancelled ctx took %v", elapsed)
	}
	// An over-cap initial is clamped by New, and doubling stays at the cap.
	if got := b.Delay(); got != Cap {
		t.Fatalf("Delay after over-cap double = %v, want %v", got, Cap)
	}
}

// The cap bounds EVERY wait, the first included, and the waits never shrink.
// An initial above Cap used to wait that long once and then "double" down to
// Cap: -otlp-retry-backoff=1m waited 1m, 30s, 30s.
func TestAnInitialAboveTheCapStartsAtTheCap(t *testing.T) {
	ctx := cancelled()
	b := New(time.Minute)
	prev := time.Duration(0)
	for i := range 4 {
		d := b.Delay()
		if d > Cap {
			t.Fatalf("wait %d is %v, past the %v cap", i, d, Cap)
		}
		if d < prev {
			t.Fatalf("wait %d (%v) is shorter than wait %d (%v): a doubling backoff must not shrink", i, d, i-1, prev)
		}
		prev = d
		b.Wait(ctx)
	}
	b.Reset()
	if got := b.Delay(); got != Cap {
		t.Fatalf("after Reset Delay = %v, want the clamped initial %v", got, Cap)
	}
}

func TestSleepWaitsTheDelay(t *testing.T) {
	b := New(10 * time.Millisecond)
	start := time.Now()
	b.Sleep(context.Background())
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Fatalf("Sleep waited %v, want >= 10ms", elapsed)
	}
}

func TestWaitReportsWhetherItRanToCompletion(t *testing.T) {
	b := New(time.Millisecond)
	if !b.Wait(context.Background()) {
		t.Fatal("Wait on a live ctx reported an early end")
	}
	if got := b.Delay(); got != 2*time.Millisecond {
		t.Fatalf("Delay after Wait = %v, want 2ms", got)
	}
	b = New(time.Minute)
	start := time.Now()
	if b.Wait(cancelled()) {
		t.Fatal("Wait on a cancelled ctx reported completion")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Wait on a cancelled ctx took %v", elapsed)
	}
}

func TestDoubleStopsAtTheCap(t *testing.T) {
	for _, tc := range []struct{ in, want time.Duration }{
		{time.Second, 2 * time.Second},
		{Cap / 2, Cap},
		{Cap/2 + 1, Cap},
		{Cap, Cap},
		{time.Hour, Cap},
	} {
		if got := Double(tc.in); got != tc.want {
			t.Errorf("Double(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
