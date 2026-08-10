package events

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// listingClient answers List with a fixed revision, which is what the cold
// skip-the-backlog start takes.
func listingClient(rv string) *fake.Clientset {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{ListMeta: metav1.ListMeta{ResourceVersion: rv}}, nil
	})
	return client
}

// The replay filter must stop at the END OF THE BACKLOG, not at the end of the
// stream. `replaying` is per-stream and survives a commit on purpose, but only
// the backlog can carry an already-exported event; the watch that follows it is
// positioned at the list's snapshot and everything it delivers is LIVE.
// Filtering that against a boundary frozen at stream start drops, for the rest
// of the watch, every event from a reporter whose clock trails it by more than
// replaySlack: silently (noteSeen has already advanced the resume point past
// it) and uncounted.
//
// The scope is now structural — the filter is applied by replayBacklog and by
// nothing else — so the assertion is on BOTH halves: the predicate stops
// filtering when the walk ends, and the watch path never consults it at all.
//
// The second half drives handle with `replaying` still SET, which stream() can
// no longer produce (replayBacklog clears the flag before the watch is opened).
// That is the point: with the flag clear, wanted() returns true for everything
// and a `if !r.wanted(e) { return nil }` restored to handle would be inert —
// the whole suite passed with that line back in. Forcing the flag is the only
// way this test can fail for the reason it names.
func TestAuditFixSecuredReplayDoesNotFilterLiveEvents(t *testing.T) {
	exp := &captureExporter{}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1})
	base := time.Now()
	r.replaying, r.replaySecured = true, false
	r.replayFrom = base

	lagging := event("lagging", "OOMKilling", "m", "Warning", "9", 1, base.Add(-5*time.Minute))
	if r.wanted(lagging) {
		t.Fatal("an in-flight backlog walk must still filter against the frozen watermark")
	}
	// replayBacklog clears the flag when the last page has been read.
	r.replaying = false
	if !r.wanted(lagging) {
		t.Fatal("a live event was dropped after the backlog was read: the filter belongs to the backlog")
	}

	// The watch path does not filter, and cannot be made to: even with the
	// backlog flag forced on and the boundary armed — the state in which
	// wanted() rejects this event outright, asserted above — handle exports it.
	r.replaying, r.replaySecured = true, false
	if err := r.handle(context.Background(), watch.Event{Type: watch.Added, Object: lagging}); err != nil {
		t.Fatal(err)
	}
	if got := len(exp.records()); got != 1 {
		t.Fatalf("exported %d records for a lagging-clock live event, want 1: the watch path must never "+
			"apply the replay watermark", got)
	}
}

// obs.EventRelists is registered as watches that "fell back to a relist". A
// watermark-less expiry does the OPPOSITE — the next stream restarts at the
// current revision and the gap is discarded — so counting it as a relist makes
// the two outcomes of expire() indistinguishable to an alert.
func TestAuditFixWatermarklessExpiryIsNotCountedAsARelist(t *testing.T) {
	client := listingClient("999")
	before := obs.EventRelists.WithLabelValues("auditfix").Value()

	// Nothing exported yet: no watermark to filter a replay, so no relist.
	r := New(Config{Client: client, StartMode: StartEnd})
	r.committed.ResourceVersion = "77"
	r.expire("auditfix")
	if r.relist {
		t.Fatal("precondition: a zero watermark cannot arm a relist")
	}
	if got := obs.EventRelists.WithLabelValues("auditfix").Value(); got != before {
		t.Fatalf("EventRelists rose by %v for an expiry that relisted nothing", got-before)
	}

	// The real fall-back still counts.
	r = New(Config{Client: client, StartMode: StartEnd})
	r.committed.ResourceVersion = "77"
	r.committed.Watermark = time.Now().Add(-time.Minute)
	r.expire("auditfix")
	if !r.relist {
		t.Fatal("precondition: an exported watermark arms the relist")
	}
	if got := obs.EventRelists.WithLabelValues("auditfix").Value(); got != before+1 {
		t.Fatalf("EventRelists delta = %v for a real relist, want 1", got-before)
	}
}

// expire must never DISARM an armed relist. loadPosition arms one with a zero
// committed watermark (unreadable position under `auto`), and an unsecured
// replay keeps its exported high-water in heldWatermark while the committed
// watermark stays zero — recomputing relist from the committed watermark alone
// read both as "nothing to recover" and restarted at the CURRENT revision,
// silently discarding the gap on exactly the recovery path.
func TestAuditFixExpireKeepsArmedRelistAndHeldExports(t *testing.T) {
	client := listingClient("999")

	// Armed by loadPosition, nothing exported: a watch that expires before
	// the replay secures must relist again, not skip the gap.
	r := New(Config{Client: client}) // StartMode defaults to auto
	r.relist = true
	before := obs.EventRelists.WithLabelValues("auditfix-armed").Value()
	r.expire("auditfix-armed")
	if !r.relist {
		t.Fatal("expire disarmed a relist armed before anything was exported")
	}
	if got := obs.EventRelists.WithLabelValues("auditfix-armed").Value(); got != before+1 {
		t.Fatalf("EventRelists delta = %v for an armed relist's expiry, want 1", got-before)
	}

	// Exports made during an UNSECURED replay live in heldWatermark with the
	// committed watermark still zero; they are exports all the same.
	r = New(Config{Client: client})
	r.heldWatermark = time.Now().Add(-time.Minute)
	r.expire("auditfix-held")
	if !r.relist {
		t.Fatal("an expiry with held (unsecured) exports must relist, not discard the gap")
	}
}

// The discard arm is the events pipeline's one silent-loss path; it must move
// a counter of its own, not just a Warn on one pod's stderr.
func TestAuditFixDiscardedGapIsCounted(t *testing.T) {
	client := listingClient("999")
	relistsBefore := obs.EventRelists.WithLabelValues("auditfix-discard").Value()
	discardsBefore := obs.EventGapDiscarded.WithLabelValues("auditfix-discard").Value()

	r := New(Config{Client: client, StartMode: StartEnd})
	r.expire("auditfix-discard")
	if r.relist {
		t.Fatal("precondition: nothing exported and nothing armed cannot relist")
	}
	if got := obs.EventGapDiscarded.WithLabelValues("auditfix-discard").Value(); got != discardsBefore+1 {
		t.Fatalf("EventGapDiscarded delta = %v for a discarded gap, want 1", got-discardsBefore)
	}
	if got := obs.EventRelists.WithLabelValues("auditfix-discard").Value(); got != relistsBefore {
		t.Fatal("a discarded gap must not count as a relist")
	}
}

// An UNREADABLE position is not a cold start. The store reports a transient
// API-server failure and an undecodable document in one shape, and under `auto`
// the cold-start policy skips to the CURRENT revision — discarding every event
// since the predecessor stopped and then overwriting the only record of where
// that was. Replay instead: duplicates, which at-least-once tolerates.
func TestAuditFixUnreadablePositionReplaysRatherThanSkipping(t *testing.T) {
	ctx := context.Background()
	client := listingClient("999")
	unreadable := failingPositions{err: errors.New("etcdserver: request timed out")}

	r := New(Config{Client: client, Positions: unreadable}) // StartMode defaults to auto
	r.loadPosition(ctx)
	start, err := r.startResourceVersion(ctx)
	if err != nil || !start.replay || !start.redelivers {
		t.Fatalf("start after an unreadable position = (%+v, %v); `auto` must replay the backlog, "+
			"not skip to the current revision", start, err)
	}

	// An explicit start mode is the operator naming a cold-start policy
	// outright, and is honoured.
	r = New(Config{Client: client, Positions: unreadable, StartMode: StartEnd})
	r.loadPosition(ctx)
	if start, err = r.startResourceVersion(ctx); err != nil || start.rv != "999" || start.replay {
		t.Fatalf("start after an unreadable position with -events-start=end = (%+v, %v), want the current revision", start, err)
	}
}

// failingPositions is a store whose Load always fails — an undecodable
// document, a 503 or a timeout are one shape here.
type failingPositions struct{ err error }

func (s failingPositions) Load(context.Context) (Position, bool, error) {
	return Position{}, false, s.err
}

func (s failingPositions) Save(context.Context, Position) error { return nil }
