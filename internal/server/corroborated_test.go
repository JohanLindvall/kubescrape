package server

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// The park measurement is a DIFFERENCE between two heap readings, so anything
// else in the process that frees memory between them is subtracted from it.
// That is not hypothetical: FuzzParkedLookupRetainsNothingButItsValidator
// measures one case straight after the previous case's teardown, and at HEAD it
// read 338, 368 and 427 B per request against a 512 B floor in 3 of 36
// whole-package runs (three concurrent processes, an ordinary CI shape) — a red
// build with no product change, on a target whose upper bound was perfectly
// happy. In the instrumented run both heap readings had SETTLED in every
// failing case, so the settle loop was never the mechanism; the released memory
// was.
//
// So the floor is corroborated: a low reading is re-taken over a fresh baseline
// and a fresh batch, and only a reading that REPEATS low is a failure. This is
// the half that proves the retry is real — the churn frees 4 MB inside the
// first attempt's window, which is what the previous case's unwinding did on a
// smaller scale, and the measurement has to survive it.
func TestAParkMeasurementSurvivesAOneOffHeapReleaseInsideItsWindow(t *testing.T) {
	if testrace.Enabled {
		t.Skip("memory measurement is meaningless under -race")
	}
	// Live at the baseline, unreachable a moment later. 4 MB is two orders of
	// magnitude over the ~85 KB of signal 64 parked requests produce, so the
	// first attempt's delta is deeply negative rather than marginally so.
	ballast := make([][]byte, 4096)
	for i := range ballast {
		ballast[i] = make([]byte, 1024)
	}
	fired := false
	heapChurn = func() {
		if fired {
			return // a ONE-OFF, which is what this noise is
		}
		fired = true
		ballast = nil
	}
	t.Cleanup(func() { heapChurn = nil })

	got, _, ok := retainedWhileParked(t, parkableHead, 64)
	if !ok {
		t.Fatal("the head this file's own seed corpus starts from did not park")
	}
	if !fired {
		t.Fatal("the churn never ran, so this test proved nothing about the retry")
	}
	if got < plausibleRequestHeap {
		t.Fatalf("the corroborated reading is %d B, under the %d B floor", got, plausibleRequestHeap)
	}
}

// …and the retry must not have turned the floor into a formality. A measurement
// that has genuinely stopped measuring reads low on EVERY attempt — that is
// exactly what distinguishes it from noise — so a release repeated per attempt
// must still fail. Without this, "retry until it passes" would be
// indistinguishable from the fix, and the floor is the only LOWER bound in this
// file: every other assertion here passes when the measurement collapses.
func TestARepeatedlyLowParkMeasurementStillFails(t *testing.T) {
	if testrace.Enabled {
		t.Skip("memory measurement is meaningless under -race")
	}
	// One ballast per attempt, ALL live when the first baseline is taken and
	// each freed inside its own attempt's window: that is what makes the low
	// reading repeat.
	ballasts := make([][][]byte, measureAttempts)
	for i := range ballasts {
		ballasts[i] = make([][]byte, 4096)
		for j := range ballasts[i] {
			ballasts[i][j] = make([]byte, 1024)
		}
	}
	attempt := 0
	heapChurn = func() {
		if attempt < len(ballasts) {
			ballasts[attempt] = nil
			attempt++
		}
	}
	t.Cleanup(func() { heapChurn = nil })

	rec := runRecorded(func(rt measureT) { _, _, _ = retainedWhileParked(rt, parkableHead, 64) })
	if rec == "" {
		t.Fatal("a park measurement that read below the floor on every one of its attempts was accepted: " +
			"the corroboration has weakened the floor into a formality, and a delta that lost the term it " +
			"reports would now pass every assertion in this file")
	}
	if !strings.Contains(rec, "measurement that stopped measuring") {
		t.Errorf("the failure does not name what it is about: %q", rec)
	}
	if attempt != measureAttempts {
		t.Errorf("the churn ran %d times, want %d: the measurement gave up before it had corroborated "+
			"anything", attempt, measureAttempts)
	}
}

// parkableHead is the seed corpus's ordinary agent poll, as a raw head.
var parkableHead = "GET /v1/containers/" + strings.Repeat("ab", 32) + "?wait=600s HTTP/1.1\r\nHost: h\r\n" +
	ordinaryPollHeaders + "\r\n"

// runRecorded runs fn against a T that RECORDS a fatal instead of failing the
// test, and returns the message (empty when fn did not fail).
//
// A subtest cannot express "this must fail": common.Fail walks up to the
// parent, so t.Run's false return arrives with the parent already failed. The
// narrow measureT interface is what makes the substitution possible, and
// *testing.T satisfies it, so the real path is unchanged.
//
// It runs fn in its own goroutine because a fatal is a runtime.Goexit, and it
// runs the registered cleanups afterwards — those are what release the parked
// handlers, which would otherwise sit in the process for the rest of the run.
func runRecorded(fn func(measureT)) string {
	rec := &recordingT{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fn(rec)
	}()
	wg.Wait()
	for i := len(rec.cleanups) - 1; i >= 0; i-- {
		rec.cleanups[i]()
	}
	return rec.failure
}

// measureT is what retainedWhileParked needs from a *testing.T.
type measureT interface {
	Helper()
	Cleanup(func())
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

type recordingT struct {
	failure  string
	cleanups []func()
}

func (r *recordingT) Helper()                   {}
func (r *recordingT) Cleanup(f func())          { r.cleanups = append(r.cleanups, f) }
func (r *recordingT) Fatal(args ...any)         { r.fatal(fmt.Sprint(args...)) }
func (r *recordingT) Fatalf(f string, a ...any) { r.fatal(fmt.Sprintf(f, a...)) }

func (r *recordingT) fatal(msg string) {
	if r.failure == "" {
		r.failure = msg
	}
	runtime.Goexit()
}
