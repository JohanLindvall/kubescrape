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
	for i := 0; i < 10; i++ {
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
	// 1m doubles to 2m and the cap clamps it inside Sleep.
	if got := b.Delay(); got != Cap {
		t.Fatalf("Delay after over-cap double = %v, want %v", got, Cap)
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
