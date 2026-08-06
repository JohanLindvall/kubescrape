package otlpexport

// Two rules the disk-buffer drain states in comments and has to keep whole:
//
//   - a nack/commit applies to the queue handle the CALLER observed, never to
//     whatever handle is installed at the moment of the call;
//   - a cancellable wait uses NewTimer+Stop, never time.After.

import (
	"os"
	"strings"
	"testing"
)

// rewindQ must roll back the reservation held on the handle it is GIVEN. With
// the handle re-read from the buffer instead, a rewind meant for a retired
// queue landed on its replacement and returned a record another reader had
// already reserved — redelivering it while the first reader still held it.
func TestRewindUsesTheCallersHandle(t *testing.T) {
	buf, err := OpenBuffer(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = buf.Close() }()

	stale, _ := buf.handles()
	for _, s := range []string{"first", "second"} {
		if err := buf.add([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	// Swap the handle out from under the caller, as a latched-I/O recovery does.
	buf.recover(stale)
	q, rd := buf.handles()
	if q == stale {
		t.Fatal("recover did not replace the queue handle")
	}
	// The NEW queue's reader takes a reservation.
	data, ok, _, err := rd.TryReserve()
	if err != nil || !ok {
		t.Fatalf("reserve on the fresh handle: ok=%v err=%v", ok, err)
	}
	if got := string(data); got != "first" {
		t.Fatalf("reserved %q, want %q", got, "first")
	}

	// A nack carrying the STALE handle must not touch that reservation.
	buf.rewindQ(stale)

	data, ok, _, err = rd.TryReserve()
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if !ok {
		t.Fatal("second reserve returned nothing")
	}
	if got := string(data); got != "second" {
		t.Fatalf("a rewind on a retired handle rolled back the live queue's reservation: re-reserved %q, want %q", got, "second")
	}
}

// The drain's read-error backoff was the one wait in this package still on
// time.After — the rule its own two neighbours state. A leaked timer is small
// (three sinks, one second) but the invariant is not conditional on the size.
func TestNoTimeAfterInThePackage(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(src), "time.After(") {
			t.Errorf("%s uses time.After: a cancelled wait must not leave the timer live until it fires (NewTimer+Stop)", name)
		}
	}
}
