package events

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// backlogClient answers List with the given pages and every Watch with a
// pre-stopped watcher that NEVER sends a bookmark — which is what
// kube-apiserver does for core/v1 events, since it serves that watch straight
// from etcd with no watch cache. It records the ListOptions and the
// resourceVersion each watch was opened at.
type backlogClient struct {
	*fake.Clientset
	lists   []metav1.ListOptions
	watches []string
	// onWatch runs when a watch is established, so a test can observe the
	// reader's state at exactly that step.
	onWatch func()
}

func newBacklogClient(t *testing.T, pages ...*corev1.EventList) *backlogClient {
	t.Helper()
	c := &backlogClient{Clientset: fake.NewSimpleClientset()}
	page := 0
	c.PrependReactor("list", "events", func(a k8stesting.Action) (bool, runtime.Object, error) {
		c.lists = append(c.lists, a.(k8stesting.ListActionImpl).ListOptions)
		if page >= len(pages) {
			t.Errorf("unexpected List #%d: the backlog had only %d pages", page+1, len(pages))
			return true, &corev1.EventList{}, nil
		}
		out := pages[page]
		page++
		return true, out, nil
	})
	c.stopWatches()
	return c
}

// stopWatches answers every Watch with a pre-stopped watcher, so the stream
// ends as soon as it establishes one — and, just as importantly, a test whose
// list step unexpectedly succeeds fails instead of blocking on a live watcher.
func (c *backlogClient) stopWatches() {
	c.PrependWatchReactor("events", func(a k8stesting.Action) (bool, watch.Interface, error) {
		c.watches = append(c.watches, a.(k8stesting.WatchActionImpl).WatchRestrictions.ResourceVersion)
		if c.onWatch != nil {
			c.onWatch()
		}
		w := watch.NewFake()
		w.Stop() // the stream sees a closed channel and returns at once
		return true, w, nil
	})
}

// backlogPage builds one list page: rv is the SNAPSHOT revision (the same on
// every page of a continued list), cont the next page's token.
func backlogPage(rv, cont string, items ...*corev1.Event) *corev1.EventList {
	out := &corev1.EventList{ListMeta: metav1.ListMeta{ResourceVersion: rv, Continue: cont}}
	for _, e := range items {
		out.Items = append(out.Items, *e)
	}
	return out
}

// THE MAJOR ONE. The replay used to be a watch started at "" whose backlog was
// only known complete when a BOOKMARK arrived — and none ever does for core/v1
// events: bookmarks are discretionary by contract, and kube-apiserver serves
// this watch straight from etcd, without the watch cache that produces them.
// So the position hold armed by every relist was permanent: the ConfigMap
// stopped advancing, and each restart replayed the whole event TTL over again,
// at a PodByName lookup per pod event.
//
// The backlog is a LIST now, so its completion is a fact this process observes.
// The position must advance to the list's snapshot revision once the backlog
// has been exported, with no bookmark anywhere in the story.
func TestReplayCommitsWithoutABookmark(t *testing.T) {
	now := time.Now()
	// Store order, NOT revision order — the shape that makes a mid-replay
	// commit unsafe in the first place.
	client := newBacklogClient(t, backlogPage("9900500", "",
		event("b", "BackOff", "m", "Warning", "9900300", 1, now.Add(-2*time.Minute)),
		event("a", "Started", "m", "Normal", "9900100", 1, now.Add(-time.Minute)),
	))
	exp := &captureExporter{}
	// BatchSize 1: every backlog entry exports as it is read, so the walk ends
	// with nothing owed and the securing is observable without the flush ticker.
	r := New(Config{Client: client, Exporter: exp, BatchSize: 1, FlushInterval: time.Hour})
	r.committed.Watermark = now.Add(-time.Hour) // something was exported before the Gone
	r.expire(stageWatch)
	if !r.relist {
		t.Fatal("precondition: the expiry must arm a relist")
	}

	if err := r.stream(context.Background()); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	if got := len(exp.records()); got != 2 {
		t.Fatalf("exported %d backlog records, want 2", got)
	}
	if got := r.committed.ResourceVersion; got != "9900500" {
		t.Fatalf("committed = %q after a fully exported backlog, want the list snapshot 9900500 — "+
			"the position stopped advancing after the first relist and every restart replays the whole event TTL", got)
	}
	if !r.replaySecured || r.relist {
		t.Fatalf("secured=%v relist=%v after a fully exported backlog, want secured with the relist disarmed",
			r.replaySecured, r.relist)
	}
	// The watch that followed the backlog is POSITIONED at the snapshot, and
	// the next stream resumes from the committed position with no second
	// backlog read.
	if len(client.watches) != 1 || client.watches[0] != "9900500" {
		t.Fatalf("watch resourceVersions = %q, want one watch positioned at the list snapshot", client.watches)
	}
	if err := r.stream(context.Background()); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	if len(client.lists) != 1 {
		t.Fatalf("the second stream re-read the backlog (%d lists total): the resume position never took effect", len(client.lists))
	}
	if len(client.watches) != 2 || client.watches[1] != "9900500" {
		t.Fatalf("watch resourceVersions = %q, want the second stream positioned at the committed position", client.watches)
	}
}

// The hold itself must survive: a position committed while backlog entries are
// still unexported points PAST them, and a restart from it never re-delivers
// them. Only "every listed entry exported" releases it — which is exactly what
// the bookmark was standing in for.
func TestReplayHoldsThePositionUntilTheBacklogIsExported(t *testing.T) {
	now := time.Now()
	client := newBacklogClient(t, backlogPage("9900500", "",
		event("b", "BackOff", "m", "Warning", "9900300", 1, now),
		event("a", "Started", "m", "Normal", "9900100", 1, now),
	))
	exp := &captureExporter{failN: 1 << 30, err: context.DeadlineExceeded}
	r := New(Config{Client: client, Exporter: exp, BatchSize: 1, FlushInterval: time.Hour})
	r.relist = true

	if err := r.stream(context.Background()); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	if got := r.committed.ResourceVersion; got != "" {
		t.Fatalf("committed = %q with the backlog unexported: a restart from it skips the backlog outright", got)
	}
	if r.replaySecured || !r.relist {
		t.Fatalf("secured=%v relist=%v with the backlog unexported, want held and re-armed", r.replaySecured, r.relist)
	}
	if r.replayOwed != 2 {
		t.Fatalf("replayOwed = %d, want the 2 unexported backlog entries", r.replayOwed)
	}

	// The collector recovers; the flush that drains the last owed entry
	// releases the hold and commits the snapshot.
	exp.mu.Lock()
	exp.failN = 0
	exp.mu.Unlock()
	for len(r.batch) > 0 {
		if err := r.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.committed.ResourceVersion; got != "9900500" {
		t.Fatalf("committed = %q once the backlog shipped, want the list snapshot 9900500", got)
	}
	if !r.replaySecured || r.relist {
		t.Fatalf("secured=%v relist=%v once the backlog shipped, want secured", r.replaySecured, r.relist)
	}
}

// The backlog is PAGED — a whole --event-ttl window is six figures on a busy
// cluster, and one unbounded List response against the singleton's 256Mi limit
// is the OOM the retained-batch cap exists to prevent, arriving before a single
// entry is exported.
//
// And the hold spans the whole walk, not each page: a page that flushes
// successfully must commit nothing, because the pages that follow carry LOWER
// revisions (a list arrives in key order) and a position taken from the first
// page's maximum strands them.
func TestReplayPagesTheBacklogAndHoldsAcrossPages(t *testing.T) {
	now := time.Now()
	client := &backlogClient{Clientset: fake.NewSimpleClientset()}
	var r *Reader
	// The position as it stood when the SECOND page was requested: page one's
	// entry has been ingested and acked by then (BatchSize 1).
	var betweenPages string
	page := 0
	client.PrependReactor("list", "events", func(a k8stesting.Action) (bool, runtime.Object, error) {
		client.lists = append(client.lists, a.(k8stesting.ListActionImpl).ListOptions)
		page++
		if page == 1 {
			return true, backlogPage("9900500", "page-2",
				event("b", "BackOff", "m", "Warning", "9900400", 1, now)), nil
		}
		betweenPages = r.committed.ResourceVersion
		return true, backlogPage("9900500", "",
			event("a", "Started", "m", "Normal", "9900100", 1, now)), nil
	})
	client.stopWatches()
	exp := &captureExporter{}
	r = New(Config{Client: client, Exporter: exp, BatchSize: 1, FlushInterval: time.Hour})
	r.relist = true

	if err := r.stream(context.Background()); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	if len(client.lists) != 2 {
		t.Fatalf("issued %d Lists, want 2 (the continue token must be followed)", len(client.lists))
	}
	if got := client.lists[0].Limit; got != replayPageSize {
		t.Errorf("first page Limit = %d, want %d: an unpaged backlog List is an OOM before anything exports", got, replayPageSize)
	}
	if got := client.lists[0].Continue; got != "" {
		t.Errorf("first page Continue = %q, want empty", got)
	}
	if got := client.lists[1].Continue; got != "page-2" {
		t.Errorf("second page Continue = %q, want the token the first page returned", got)
	}
	if got := len(exp.records()); got != 2 {
		t.Fatalf("exported %d records, want both pages' entries", got)
	}
	if betweenPages != "" {
		t.Fatalf("committed %q after the first page's ack; the lower-revision entries of page 2 were still "+
			"undelivered and a restart from that position loses them", betweenPages)
	}
	if got := r.committed.ResourceVersion; got != "9900500" {
		t.Fatalf("committed = %q after the whole walk, want the snapshot 9900500", got)
	}
}

// A continue token pins a snapshot that ages out like any other revision. The
// pages already read cannot be completed from a second snapshot, so the replay
// is abandoned and RE-ARMED (counted, like a Gone watch) rather than half
// applied — a boundary committed over a partial walk is the same silent loss.
//
// The assertions are the things expire() UNIQUELY does, because the rest of
// this state is already true without it: relist was armed by the caller and
// expire never disarms one, committed.ResourceVersion is empty because the hold
// never let it advance, and replaySecured is false for the whole walk. Deleting
// the isExpired/expire block left the entire suite green. What only expire does
// is count the fall-back under its STAGE label and clear the in-process resume
// point — a revision the API server has dropped must not be watched from again.
func TestReplayContinuationExpiryRearmsTheRelist(t *testing.T) {
	now := time.Now()
	client := &backlogClient{Clientset: fake.NewSimpleClientset()}
	page := 0
	client.PrependReactor("list", "events", func(a k8stesting.Action) (bool, runtime.Object, error) {
		client.lists = append(client.lists, a.(k8stesting.ListActionImpl).ListOptions)
		page++
		if page == 1 {
			return true, backlogPage("9900500", "page-2",
				event("b", "BackOff", "m", "Warning", "9900400", 1, now)), nil
		}
		return true, nil, apierrors.NewResourceExpired("continue token expired")
	})
	client.stopWatches()
	exp := &captureExporter{}
	r := New(Config{Client: client, Exporter: exp, BatchSize: 1000, FlushInterval: time.Hour})
	r.committed.Watermark = now.Add(-time.Hour)
	r.relist = true
	// A revision this process saw before the relist. It is dead the moment the
	// walk's own snapshot expires — the same window aged both out — and a
	// stream resumed from it would fail Gone again, or worse, be accepted and
	// skip the gap.
	r.seenRV = "9900450"
	relistsBefore := obs.EventRelists.WithLabelValues(stageReplay).Value()

	err := r.stream(context.Background())
	if err == nil {
		t.Fatal("an expired continuation must fail the stream")
	}
	if got := obs.EventRelists.WithLabelValues(stageReplay).Value(); got != relistsBefore+1 {
		t.Errorf("EventRelists{stage=%q} delta = %v, want 1: the expiry of a continue token mid-walk is a "+
			"relist fall-back and is the only thing that writes that label", stageReplay, got-relistsBefore)
	}
	if r.seenRV != "" {
		t.Errorf("seenRV = %q after the snapshot expired; the next stream resumes a watch from a revision "+
			"the API server has already dropped", r.seenRV)
	}
	if r.committed.ResourceVersion != "" {
		t.Fatalf("committed = %q from a half-read backlog", r.committed.ResourceVersion)
	}
	if !r.relist || r.replaySecured {
		t.Fatalf("relist=%v secured=%v after an expired continuation, want the replay re-armed",
			r.relist, r.replaySecured)
	}
	start, serr := r.startResourceVersion(context.Background())
	if serr != nil || !start.replay {
		t.Fatalf("next start = (%+v, %v), want another replay", start, serr)
	}
}

// Backlog entries shed at the retained-batch cap can never be exported, so the
// owed count must retire with them. Holding the position for entries that are
// already counted loss wedges it exactly as waiting for a bookmark did.
func TestShedBacklogEntriesStopBeingOwed(t *testing.T) {
	r, _, _ := newReader(t, Config{BatchSize: 1000})
	const n = shedChunk + 40
	for i := 0; i < n; i++ {
		r.batch = append(r.batch, entry{rv: strconv.Itoa(100 + i)})
	}
	r.replaying, r.replaySecured = false, false
	r.replayOwed = n
	r.replayRV = "9900500"

	r.shedOldest() // drops [0, shedChunk) — the whole prefix is unrendered
	if want := n - shedChunk; r.replayOwed != want {
		t.Fatalf("replayOwed = %d after shedding %d entries, want %d", r.replayOwed, shedChunk, want)
	}
	if r.replaySecured {
		t.Fatal("shedding is not exporting: the remaining owed entries still hold the position")
	}

	// Draining the remainder secures the replay rather than leaving it stuck.
	r.settle(entry{rv: "9900400", when: time.Now()}, len(r.batch))
	if !r.replaySecured || r.committed.ResourceVersion != "9900500" {
		t.Fatalf("secured=%v committed=%q, want the snapshot committed once nothing is owed",
			r.replaySecured, r.committed.ResourceVersion)
	}
}

// A bookmark is a strictly stronger statement than "the backlog was exported",
// so a cluster that DOES send one still secures the replay early. Nothing may
// depend on it arriving, but the path must not rot either.
//
// The state is FORCED, and unreachable as the code stands: an unsecured replay
// means entries are still owed, owed entries are batch entries, and a bookmark
// arriving with a non-empty batch stays pending rather than securing anything.
// That is why this is written as a direct drive of the securing branch and not
// as a stream — it pins the branch, not a scenario.
func TestBookmarkStillSecuresAReplay(t *testing.T) {
	r, _, _ := newReader(t, Config{Exporter: &captureExporter{}})
	r.replaySecured = false
	r.replayRV = "9900500"
	r.heldWatermark = time.Now().Add(-time.Minute)

	bm := &corev1.Event{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "9900600"}}
	if err := r.handle(context.Background(), watch.Event{Type: watch.Bookmark, Object: bm}); err != nil {
		t.Fatal(err)
	}
	if !r.replaySecured {
		t.Fatal("a bookmark must still secure the replay")
	}
	if got := r.committed.ResourceVersion; got != "9900600" {
		t.Fatalf("committed = %q, want the bookmark's revision", got)
	}
	if r.committed.Watermark.IsZero() {
		t.Fatal("the held watermark must fold in when the replay secures")
	}
}

// THE SECOND MAJOR ONE. The walk blocks the single reader goroutine, so
// nothing services the FlushInterval ticker until it returns: the COUNT trigger
// is the only export there is. -events-batch-size has no upper bound and the
// retention cap does not follow it past maxRetainedCeiling, so a BatchSize
// above the cap made that trigger UNREACHABLE — the batch sat at the cap and
// shed the whole backlog, unexported, into a counter documented as outright
// loss, before the watch was even opened. Measured on a 30,000-event backlog:
// BatchSize=512 exported 29,696 and shed 0; BatchSize=20000 exported 0 and shed
// 13,696.
func TestBacklogIsNotShedWhenBatchSizeExceedsTheRetainCap(t *testing.T) {
	now := time.Now()
	const batchSize = 20000
	// The cap is derived from BatchSize, so ask a reader for it before sizing
	// the backlog against it. One entry past the cap is all it takes to shed;
	// go a chunk further so a half-fixed trigger (one that sheds once and then
	// recovers) still fails.
	sizing := New(Config{BatchSize: batchSize})
	if batchSize <= sizing.retainCap() {
		t.Fatalf("precondition: BatchSize %d must exceed the retain cap %d for the trigger to be unreachable",
			batchSize, sizing.retainCap())
	}
	total := sizing.retainCap() + shedChunk + 50
	// The snapshot revision is above every item's, as a real list's is.
	client := pagedBacklogClient(t, "9999999", total, func(i int) *corev1.Event {
		return event("e"+strconv.Itoa(i), "R", "m", "Normal", strconv.Itoa(9900000+i), 1, now)
	})
	exp := &captureExporter{}
	r := New(Config{Client: client, Exporter: exp, BatchSize: batchSize, FlushInterval: time.Hour})
	r.relist = true

	shedBefore := obs.EventsOverflowDropped.Value()
	if err := r.stream(context.Background()); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	if shed := obs.EventsOverflowDropped.Value() - shedBefore; shed != 0 {
		t.Errorf("the walk shed %v of %d backlog entries: the count trigger is unreachable at BatchSize=%d "+
			"(cap %d) and the walk services no ticker, so the backlog is dropped before the watch opens",
			shed, total, r.cfg.BatchSize, r.retainCap())
	}
	if got := len(exp.records()); got != total {
		t.Errorf("exported %d of %d backlog records", got, total)
	}
	if !r.replaySecured || r.committed.ResourceVersion != "9999999" {
		t.Errorf("secured=%v committed=%q after a fully exported backlog, want the snapshot committed",
			r.replaySecured, r.committed.ResourceVersion)
	}
}

// Every walk leaves a TAIL the count trigger never reaches — up to flushAt
// entries — and the next export opportunity is the FlushInterval ticker inside
// the watch loop, i.e. only if establishing the watch succeeds. That is exactly
// the step the walk-aged snapshot revision can fail, and owed entries hold the
// position, so a Gone watch there re-lapped the whole backlog with nothing
// committed and nothing filtering it. The tail must be exported, and the replay
// secured, BEFORE the watch is opened.
func TestReplayExportsTheTailBeforeOpeningTheWatch(t *testing.T) {
	now := time.Now()
	client := newBacklogClient(t, backlogPage("9900500", "",
		event("a", "Started", "m", "Normal", "9900100", 1, now),
		event("b", "BackOff", "m", "Warning", "9900300", 1, now),
	))
	exp := &captureExporter{}
	// BatchSize 1000: the whole backlog is the tail — nothing triggers on count.
	r := New(Config{Client: client, Exporter: exp, BatchSize: 1000, FlushInterval: time.Hour})
	r.relist = true

	// What the reader had shipped at the moment the watch was established.
	var exportedAtWatch, owedAtWatch int
	var securedAtWatch bool
	client.onWatch = func() {
		exportedAtWatch, owedAtWatch, securedAtWatch = len(exp.records()), r.replayOwed, r.replaySecured
	}

	if err := r.stream(context.Background()); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	if exportedAtWatch != 2 || owedAtWatch != 0 || !securedAtWatch {
		t.Fatalf("at the watch: exported=%d owed=%d secured=%v, want the tail shipped and the replay secured "+
			"(a failure opening the watch strands them, and the position is held for as long as they are owed)",
			exportedAtWatch, owedAtWatch, securedAtWatch)
	}
}

// A completed lap must bound the next one. The watch after the walk is
// positioned at a snapshot revision taken BEFORE it, so the walk's duration is
// charged against the API server's compaction window — the accepted cost of the
// list-then-watch shape (client-go's own reflector has it). The recovery is
// another relist, and what keeps that from re-exporting the whole backlog lap
// after lap is that the completed lap commits its watermark: the next lap's
// wanted() drops everything already shipped, bar the replaySlack window that
// biases this pipeline toward duplicates over loss.
func TestGoneWatchAfterAReplayDoesNotReExportTheBacklog(t *testing.T) {
	now := time.Now()
	backlog := []*corev1.Event{
		event("old", "Started", "m", "Normal", "9900100", 1, now.Add(-time.Hour)),
		event("mid", "Pulled", "m", "Normal", "9900200", 1, now.Add(-30*time.Minute)),
		event("new", "BackOff", "m", "Warning", "9900300", 1, now.Add(-2*time.Minute)),
	}
	client := &backlogClient{Clientset: fake.NewSimpleClientset()}
	client.PrependReactor("list", "events", func(a k8stesting.Action) (bool, runtime.Object, error) {
		client.lists = append(client.lists, a.(k8stesting.ListActionImpl).ListOptions)
		return true, backlogPage("9900500", "", backlog...), nil
	})
	// The snapshot aged out while the walk ran: the watch cannot be positioned
	// at it. That is the failure this shape exposes and must survive.
	client.PrependWatchReactor("events", func(a k8stesting.Action) (bool, watch.Interface, error) {
		client.watches = append(client.watches, a.(k8stesting.WatchActionImpl).WatchRestrictions.ResourceVersion)
		return true, nil, apierrors.NewResourceExpired("too old resource version")
	})
	exp := &captureExporter{}
	r := New(Config{Client: client, Exporter: exp, BatchSize: 1000, FlushInterval: time.Hour})
	r.relist = true

	if err := r.stream(context.Background()); err == nil {
		t.Fatal("an expired watch revision must fail the stream")
	}
	if got := len(exp.records()); got != 3 {
		t.Fatalf("first lap exported %d records, want the whole backlog", got)
	}
	if !r.relist {
		t.Fatal("the Gone watch must arm another relist")
	}

	// The second lap re-reads the same backlog. Only the entries inside the
	// slack window of the committed watermark may ship again.
	if err := r.stream(context.Background()); err == nil {
		t.Fatal("an expired watch revision must fail the stream")
	}
	shipped := map[string]int{}
	for _, rec := range exp.records() {
		v, ok := rec.Attributes().Get("k8s.event.name")
		if !ok {
			t.Fatal("record without k8s.event.name")
		}
		shipped[v.Str()]++
	}
	for _, name := range []string{"old", "mid"} {
		if shipped[name] != 1 {
			t.Errorf("%q shipped %d times across two laps, want 1: a completed lap commits the watermark, "+
				"so the relist after a Gone watch must not re-export what it already delivered",
				name, shipped[name])
		}
	}
	if shipped["new"] != 2 {
		t.Errorf("%q shipped %d times, want 2 — the replaySlack window is the documented duplicate bias",
			"new", shipped["new"])
	}
}

// pagedBacklogClient serves n generated events in replayPageSize pages off one
// snapshot revision, and answers every watch with a pre-stopped watcher.
func pagedBacklogClient(t *testing.T, rv string, n int, mk func(int) *corev1.Event) *backlogClient {
	t.Helper()
	c := &backlogClient{Clientset: fake.NewSimpleClientset()}
	next := 0
	c.PrependReactor("list", "events", func(a k8stesting.Action) (bool, runtime.Object, error) {
		c.lists = append(c.lists, a.(k8stesting.ListActionImpl).ListOptions)
		from := next
		next = min(from+replayPageSize, n)
		cont := "page-" + strconv.Itoa(next)
		if next >= n {
			cont = ""
		}
		page := backlogPage(rv, cont)
		for i := from; i < next; i++ {
			page.Items = append(page.Items, *mk(i))
		}
		return true, page, nil
	})
	c.stopWatches()
	return c
}

// A continue token that does not advance is a walk that never terminates, and
// it holds the single reader goroutine. It must be an ERROR rather than a
// break: breaking would secure the replay over a backlog only partly read,
// which is precisely the silent loss the position hold exists to prevent.
func TestReplayRefusesANonAdvancingContinueToken(t *testing.T) {
	now := time.Now()
	client := &backlogClient{Clientset: fake.NewSimpleClientset()}
	client.PrependReactor("list", "events", func(a k8stesting.Action) (bool, runtime.Object, error) {
		client.lists = append(client.lists, a.(k8stesting.ListActionImpl).ListOptions)
		if len(client.lists) > 8 {
			t.Fatal("the backlog walk did not stop on a repeated continue token")
		}
		return true, backlogPage("9900500", "stuck",
			event("b", "BackOff", "m", "Warning", "9900400", 1, now)), nil
	})
	client.stopWatches()
	r := New(Config{Client: client, Exporter: &captureExporter{}, BatchSize: 1000, FlushInterval: time.Hour})
	r.relist = true

	err := r.stream(context.Background())
	if err == nil || !strings.Contains(err.Error(), "continue token") {
		t.Fatalf("err = %v, want the walk refused for making no progress", err)
	}
	if r.committed.ResourceVersion != "" || r.replaySecured {
		t.Fatalf("committed=%q secured=%v; a half-read backlog must commit nothing",
			r.committed.ResourceVersion, r.replaySecured)
	}
}
