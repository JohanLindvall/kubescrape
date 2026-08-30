package selfmeta

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// waitFor polls until cond holds, so a background resolver's output is read
// without sleeping for a fixed guess.
func waitFor(t *testing.T, out *syncBuffer, what string, cond func(string) bool) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if s := out.String(); cond(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; log so far:\n%s", what, out.String())
	return ""
}

// The RECOVERY line, which did not exist: a Warn that simply stops cannot be
// told from a lookup that is still broken and nobody is watching. It also has
// to name the outage, because the value stamped on every export in between was
// the stale one.
func TestPollReportsRecoveryAfterAFailedLookup(t *testing.T) {
	var out syncBuffer
	log := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var calls atomic.Int64
	resolve := func(context.Context) (*kubemeta.Pod, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("dial tcp 10.0.0.1:80: connect: connection refused")
		}
		return testPod(), nil
	}
	// A short retry so the second attempt lands without the test sleeping
	// through firstRetry's five seconds.
	prev := firstRetry
	firstRetry = time.Millisecond
	t.Cleanup(func() { firstRetry = prev })

	get := startPoll(t, resolve, PollConfig[kubemeta.Pod]{
		Refresh: time.Minute, What: "this pod's own metadata", Log: log,
	})
	line := waitFor(t, &out, "the recovery line", func(s string) bool {
		return strings.Contains(s, "succeeded again")
	})
	if !strings.Contains(line, `level=WARN msg="resolving this pod's own metadata failed`) {
		t.Errorf("the first failure must warn and name WHAT failed:\n%s", line)
	}
	if !strings.Contains(line, "attempts=1") {
		t.Errorf("the recovery must say how many attempts the outage cost:\n%s", line)
	}
	if p := get(); p == nil {
		t.Error("the provider must serve the resolved value after the recovery")
	}
}

// One message used to serve BOTH the pod lookup and the node lookup, so a
// failure of GET /v1/nodes/{name}/metadata read "resolving own metadata
// failed" and pointed the operator at /v1/self.
func TestPollNamesWhatItIsResolving(t *testing.T) {
	for _, tc := range []struct{ name, what, want string }{
		{"explicit", "this node's metadata", "resolving this node's metadata failed"},
		// With nothing configured the type is at least unambiguous, which is
		// what makes the fallback safe for a caller that forgets.
		{"fallback to the type", "", "resolving kubemeta.Pod failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out syncBuffer
			log := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
			prev := firstRetry
			firstRetry = time.Millisecond
			t.Cleanup(func() { firstRetry = prev })
			startPoll(t, func(context.Context) (*kubemeta.Pod, error) {
				return nil, errors.New("nope")
			}, PollConfig[kubemeta.Pod]{Refresh: time.Minute, What: tc.what, Log: log})
			waitFor(t, &out, tc.want, func(s string) bool { return strings.Contains(s, tc.want) })
		})
	}
}

// A lookup that never succeeds must not flood: after the first Warn the
// repeats are Debug until the re-warn interval elapses. (The re-warn itself is
// on real time — logdedupe.Throttle owns that clock — so what is pinned here
// is the quiet, which is the half that costs an operator money.)
func TestPollRepeatFailuresStayQuiet(t *testing.T) {
	var out syncBuffer
	log := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	prev := firstRetry
	firstRetry = time.Millisecond
	t.Cleanup(func() { firstRetry = prev })
	var calls atomic.Int64
	startPoll(t, func(context.Context) (*kubemeta.Pod, error) {
		calls.Add(1)
		return nil, errors.New("nope")
	}, PollConfig[kubemeta.Pod]{Refresh: time.Minute, What: "this pod's own metadata", Log: log})

	waitFor(t, &out, "several attempts", func(string) bool { return calls.Load() >= 5 })
	if n := strings.Count(out.String(), "level=WARN"); n != 1 {
		t.Errorf("WARN lines = %d after %d attempts, want exactly 1:\n%s", n, calls.Load(), out.String())
	}
}
