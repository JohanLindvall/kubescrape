package tailer

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/route"
)

// reofferProbe records whether each export arrived marked route.Reoffer.
type reofferProbe struct{ marked []bool }

func (p *reofferProbe) ExportLogs(ctx context.Context, _ plog.Logs) error {
	p.marked = append(p.marked, route.Reoffered(ctx))
	return nil
}

// Every tailer export is marked route.Reoffer: a failed flush rewinds its files
// and the next sweep re-sends the same records, which is the promise that lets
// a router splitting the batch hold its default share back while a tenant
// route is down, instead of spooling one copy of it per attempt.
func TestFlushExportsAreMarkedReoffered(t *testing.T) {
	tl, f := benchTailer(t, Config{Multiline: true})
	probe := &reofferProbe{}
	tl.cfg.Exporter = probe
	feedAll(tl, f, benchLines(8))
	tl.flush(context.Background())
	if len(probe.marked) == 0 {
		t.Fatal("the flush exported nothing; the fixture no longer reaches the exporter")
	}
	for i, m := range probe.marked {
		if !m {
			t.Errorf("export %d was not marked route.Reoffer", i)
		}
	}
}
