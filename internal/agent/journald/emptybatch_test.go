package journald

import (
	"context"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// An all-dropped batch must commit WITHOUT exporting: a record-less payload
// still costs a wire RPC every flush interval (and an fsync'd spool frame under
// -buffer-dir) on exactly the heavily-sampled journal the rules exist for.
func TestProbeAllDroppedBatchIsNotExported(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{{Action: "drop", MatchRegexp: []string{"__line__=noise"}}})
	if err != nil {
		t.Fatal(err)
	}
	entries := []rawEntry{
		mkEntry("c1", "kubelet.service", "noise one", "6"),
		mkEntry("c2", "kubelet.service", "noise two", "6"),
	}
	exp := &captureExporter{}
	r := New(Config{Exporter: exp, FlushInterval: 20 * time.Millisecond, Rules: rules})
	r.open = fakeOpener(entries, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	time.Sleep(150 * time.Millisecond)
	exp.mu.Lock()
	n := len(exp.batches)
	exp.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d export calls for an all-dropped batch; want none", n)
	}
}
