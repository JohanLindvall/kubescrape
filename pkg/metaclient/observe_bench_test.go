package metaclient

// observe() runs once per metadata lookup — on the concurrent ingest
// enrichment, the cadvisor batchers and every file the tailer resolves — so it
// is the one function in this package whose cost is paid per lookup rather than
// per response. BenchmarkCacheHitPod/Parallel measure the whole lookup, which
// on a loaded machine has a ±10-20% noise floor and cannot resolve a change of
// this size; these isolate it.
//
// The parallel arm is not decoration: the outcome tallies are five atomics in
// one struct, so a serial measurement understates what a fleet of concurrent
// ingest handlers pays.

import (
	"log/slog"
	"os"
	"testing"
)

func benchObserveClient() *Client {
	// An INFO logger, which is the shipped shape: the summary this function
	// gates is Debug-only, so the interesting cost is the disabled path.
	return New(Config{Base: "http://x", Log: slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelInfo}))})
}

func BenchmarkObserve(b *testing.B) {
	b.Run("cached", func(b *testing.B) {
		c := benchObserveClient()
		b.ReportAllocs()
		for b.Loop() {
			c.observe(OutcomeCached)
		}
	})
	b.Run("cachedParallel", func(b *testing.B) {
		c := benchObserveClient()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.observe(OutcomeCached)
			}
		})
	})
	b.Run("withHook", func(b *testing.B) {
		var n int
		c := New(Config{Base: "http://x", Observe: func(string) { n++ },
			Log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))})
		b.ReportAllocs()
		for b.Loop() {
			c.observe(OutcomeNotModified)
		}
		_ = n
	})
}
