package journald

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// An all-dropped batch must commit WITHOUT exporting: a record-less payload
// still costs a wire RPC every flush interval (and an fsync'd spool frame under
// -buffer-dir) on exactly the heavily-sampled journal the rules exist for.
//
// It waits for the COMMIT rather than sleeping: a fixed sleep followed by "no
// export happened" also passed for a reader that never ingested or flushed at
// all.
func TestAllDroppedBatchCommitsWithoutExporting(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{{Action: "drop", MatchRegexp: []string{"__line__=noise"}}})
	if err != nil {
		t.Fatal(err)
	}
	entries := []rawEntry{
		mkEntry("c1", "kubelet.service", "noise one", "6"),
		mkEntry("c2", "kubelet.service", "noise two", "6"),
	}
	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "positions.json"))
	exp := &captureExporter{}
	r := New(Config{Exporter: exp, FlushInterval: 20 * time.Millisecond, Chain: logchain.Config{Rules: rules}, Positions: pos})
	r.open = fakeOpener(entries, false)
	r.cursorPersistEvery = 0 // see startReader
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, "the all-dropped batch committed", func() bool { return pos.JournalCursor() == "c2" })
	// attempts counts every call, refused ones included: stricter than the
	// delivered batches.
	if n := exp.attempts(); n != 0 {
		t.Fatalf("%d export calls for an all-dropped batch; want none", n)
	}
}
