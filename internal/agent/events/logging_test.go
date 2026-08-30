package events

// What the operator can SEE when the events pipeline degrades. Every case here
// is a path that used to move a counter (or nothing at all) with no line
// carrying the object, the error or the remedy.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// capturedLog is a logger writing into a buffer, at Debug so both halves of a
// counter+context pair are visible to a test.
func capturedLog() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

// failingMeta is a metadata source that cannot answer.
type failingMeta struct{ err error }

func (f failingMeta) PodByName(context.Context, string, string) (*kubemeta.Pod, error) {
	return nil, f.err
}

// The whole reason events are collected here rather than as a flat stream is
// that an event about a pod lands on that pod's resource attributes. When that
// resolution fails the event still exports — so nothing else can report it.
func TestUnresolvedInvolvedPodIsCountedAndLogged(t *testing.T) {
	for _, tc := range []struct {
		name   string
		meta   MetadataSource
		reason string
	}{
		{"lookup", failingMeta{err: errors.New("metadata service unreachable")}, reasonLookup},
		// A pod of that name exists but is a different incarnation: a
		// SUCCESSFUL lookup, so no request counter can show it either.
		{"uid mismatch", fakeMeta{pod: &kubemeta.Pod{UID: "someone-else", Name: "web-abc"}}, reasonUIDMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log, dump := capturedLog()
			before := obs.EventsUnresolved.WithLabelValues(tc.reason).Value()
			r, _, _ := newReader(t, Config{Meta: tc.meta, Logger: log, BatchSize: 100})
			ctx := context.Background()
			r.ingest(ctx, event("a", "OOMKilled", "boom", "Warning", "10", 1, time.Now()))

			if got := obs.EventsUnresolved.WithLabelValues(tc.reason).Value(); got <= before {
				t.Errorf("kubescrape_events_unresolved_total{reason=%q} did not move (%v -> %v)", tc.reason, before, got)
			}
			out := dump()
			if !strings.Contains(out, "reason="+tc.reason) || !strings.Contains(out, "pod=web-abc") {
				t.Errorf("no line naming the object and the reason:\n%s", out)
			}
			// The condition is a state, so it is warned once and then throttled
			// — but it must be warned at least once, or the counter is the only
			// evidence and it cannot say which pod.
			if !strings.Contains(out, "level=WARN") {
				t.Errorf("the condition was never warned about:\n%s", out)
			}
		})
	}
}

// A resolvable pod logs nothing: this pipeline's Debug must not become a line
// per event on a healthy cluster.
func TestResolvedInvolvedPodIsSilent(t *testing.T) {
	log, dump := capturedLog()
	r, _, _ := newReader(t, Config{
		Meta:   fakeMeta{pod: &kubemeta.Pod{UID: "pod-uid", Name: "web-abc", Namespace: "default"}},
		Logger: log, BatchSize: 100,
	})
	r.ingest(context.Background(), event("a", "Started", "ok", "Normal", "10", 1, time.Now()))
	if out := dump(); out != "" {
		t.Errorf("a resolved event logged something:\n%s", out)
	}
}

// failingStore fails every Save; Load reports a cold start.
type failingStore struct{ fail bool }

func (s *failingStore) Load(context.Context) (Position, bool, error) { return Position{}, false, nil }

func (s *failingStore) Save(context.Context, Position) error {
	if s.fail {
		return errors.New("configmaps is forbidden")
	}
	return nil
}

// The position is the one piece of state that outlives the process, and an
// unwritable one is silent loss of the RESUME POINT: the pipeline keeps
// exporting and a handover then replays (or, past the watch window, discards).
// One Warn on the transition, one Info on the recovery, and a Debug per write.
func TestPositionWriteFailureWarnsOnceAndRecovers(t *testing.T) {
	log, dump := capturedLog()
	store := &failingStore{fail: true}
	r, _, _ := newReader(t, Config{Positions: store, Logger: log, PersistInterval: time.Nanosecond})
	r.committed.ResourceVersion = "42"
	ctx := context.Background()

	r.persist(ctx, true)
	r.persist(ctx, true) // the repeat must be throttled, not a second line
	out := dump()
	if n := strings.Count(out, "writing the event position failed"); n != 1 {
		t.Errorf("want exactly one transition warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "resourceVersion=42") {
		t.Errorf("the warning does not say where the position stood:\n%s", out)
	}

	store.fail = false
	r.persist(ctx, true)
	out = dump()
	if !strings.Contains(out, "writing the event position recovered") {
		t.Errorf("the recovery is silent, which reads the same as a still-broken write:\n%s", out)
	}
	if !strings.Contains(out, "event position written") {
		t.Errorf("a successful write leaves no Debug trace:\n%s", out)
	}
}

// The overflow warning used to latch for the process' life, so a singleton
// running for weeks reported only its FIRST outage. It re-arms on the flush
// that recovers.
func TestOverflowWarningReArmsAfterRecovery(t *testing.T) {
	r, _, _ := newReader(t, Config{BatchSize: 1})
	r.overflowWarned = true
	r.flushFailedAt = time.Now().Add(-time.Minute)
	r.tryFlush(context.Background()) // an empty batch flushes cleanly
	if r.overflowWarned {
		t.Error("the overflow warning stayed latched after the export recovered")
	}
}

// A watch that decodes into something other than an Event exports nothing while
// looking perfectly healthy. Throttled, because it would be one line per
// delivery.
func TestNonEventObjectIsWarnedAndThrottled(t *testing.T) {
	log, dump := capturedLog()
	r, _, _ := newReader(t, Config{Logger: log, BatchSize: 100})
	ctx := context.Background()
	odd := watch.Event{Type: watch.Added, Object: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "not-an-event"}}}
	for i := 0; i < 3; i++ {
		if err := r.handle(ctx, odd); err != nil {
			t.Fatal(err)
		}
	}
	out := dump()
	if n := strings.Count(out, "is not a core/v1 Event"); n != 1 {
		t.Errorf("want exactly one (throttled) warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "type=*v1.Pod") {
		t.Errorf("the warning does not name what arrived:\n%s", out)
	}
}

// uidEvent is `event` with a chosen involved-object UID.
func uidEvent(uid types.UID) *corev1.Event {
	e := event("a", "Started", "ok", "Normal", "10", 1, time.Now())
	e.InvolvedObject.UID = uid
	return e
}

// An event whose involved object carries NO uid cannot be mismatched, so it
// must resolve rather than be counted unresolved — the cross-check is a guard
// against adopting the wrong pod, not a requirement that every event carry a
// uid.
func TestInvolvedObjectWithoutUIDStillResolves(t *testing.T) {
	log, dump := capturedLog()
	r, _, _ := newReader(t, Config{
		Meta:   fakeMeta{pod: &kubemeta.Pod{UID: "pod-uid", Name: "web-abc", Namespace: "default"}},
		Logger: log, BatchSize: 100,
	})
	r.ingest(context.Background(), uidEvent(""))
	if out := dump(); strings.Contains(out, "did not resolve") {
		t.Errorf("a uid-less involved object was reported unresolved:\n%s", out)
	}
}
