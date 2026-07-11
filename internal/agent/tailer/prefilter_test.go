package tailer

import (
	"testing"

	"github.com/JohanLindvall/multiline/patterns"
)

// Representative first lines for every bundled trace format, plus ordinary
// lines. The prefiltered matcher must agree with the unwrapped state machine
// on every one of them (from the start state — the only state the prefilter
// short-circuits).
var prefilterCorpus = []string{
	// go
	"panic: runtime error: invalid memory address or nil pointer dereference",
	"2026/07/11 10:00:00 http: panic serving 10.0.0.5:1234: oops",
	// java / node
	"java.lang.IllegalStateException: boom",
	"Exception in thread \"main\" java.lang.RuntimeException: x",
	"TypeError: Cannot read property 'x' of undefined",
	"V8 errors stack trace: something",
	"com.example.FooThrowable: wrapped",
	// python
	"Traceback (most recent call last):",
	// dotnet
	"Unhandled exception. System.InvalidOperationException: fail",
	// ruby
	"app.rb:12:in `divide': divided by 0 (ZeroDivisionError)",
	// rust
	"thread 'main' panicked at src/main.rs:4:5:",
	// php
	"PHP Fatal error:  Uncaught RuntimeException: nope",
	"Fatal error: Uncaught Error: Call to undefined function",
	// ordinary lines that must NOT start a group
	`{"level":"info","msg":"handled request","http_status":200}`,
	"GET /api/v1/orders 200 42.5ms",
	"error contacting upstream: connection refused",
	"user retried the transaction",
	"level=debug msg=\"cache lookup\" hit=true",
	"",
	"    indented continuation-looking line",
	"goroutine 12 [running]:", // continuation shape, not a start
}

func TestPrefilterMatchesUnfiltered(t *testing.T) {
	pm, ok := traceMatcher.(*prefilteredMatcher)
	if !ok {
		t.Fatal("prefilter unexpectedly disabled (literal extraction failed)")
	}
	t.Logf("literals: %q", pm.literals)
	inner := patterns.MustCompile(patterns.All...)
	start := []int{0}
	for _, line := range prefilterCorpus {
		wantNext, wantAcc := inner.Step(line, start)
		gotNext, gotAcc := pm.Step(line, start)
		if len(gotNext) != len(wantNext) || gotAcc != wantAcc {
			t.Errorf("Step(%q) = %v,%d want %v,%d", line, gotNext, gotAcc, wantNext, wantAcc)
		}
	}
}

// Every start-state pattern's required literals must be provable — if this
// fails after a multiline upgrade, the prefilter silently disabled itself and
// the per-line cost regresses ~3x (it stays CORRECT; this test is the alarm).
func TestPrefilterEnabled(t *testing.T) {
	lits, ok := startLiterals(patterns.All)
	if !ok || len(lits) == 0 {
		t.Fatalf("startLiterals = %q, %v", lits, ok)
	}
}
