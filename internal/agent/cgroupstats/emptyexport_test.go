package cgroupstats

import (
	"context"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/pdatacheck"
)

// A container reaches the export snapshot only when at least one of its two
// signals has something to say (`if cpu.emit || mem.emit` in the snapshot
// pass), and build renders each signal's five gauges under its own `emit`.
// Those two facts are two hundred lines apart. If the first were relaxed, the
// second would ship a ResourceMetrics carrying this pipeline's full container
// identity — eighteen attributes, deliberately byte-identical to the cadvisor
// row's — plus a named scope and NOT ONE MEASUREMENT. Nothing downstream
// rejects that, so nothing would ever report it.
//
// The MIXED payload is what this has to be tested on. export's own
// `DataPointCount() == 0` short-circuit covers a payload that is empty in
// total, so a lone silent container proves nothing: it is a second container
// with a real window that makes the payload shippable and would carry the
// silent one's empty envelope out with it.
func TestASilentContainerIsNotExportedBesideATalkingOne(t *testing.T) {
	h := newHarness(t)
	talking := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, talking, 0, 100<<20, 0)
	h.discover()

	// Two sweeps give the first container a real window (a CPU rate needs two
	// raw readings), which is what makes the payload shippable at all.
	setUsage(t, talking, 2_000_000)
	setMem(t, talking, 200<<20, 0)
	h.advance(time.Second)
	setUsage(t, talking, 3_000_000)
	setMem(t, talking, 300<<20, 0)
	h.advance(time.Second)

	// The second container appears now, so its first window holds one memory
	// reading and no CPU rate at all: neither signal can describe a
	// distribution and there is no held value to re-state yet.
	silent := systemdContainerDir(h.root, 2, hexID(2))
	makeContainer(t, silent, 0, 50<<20, 0)
	h.discover()
	h.advance(time.Second)

	exp := &fakeExporter{}
	if err := h.export(context.Background(), exp); err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(exp.sent) == 0 {
		t.Fatal("nothing was exported, so this proves nothing: the talking container's window should have shipped")
	}

	sawTalking := false
	for _, md := range exp.sent {
		if bad := pdatacheck.EmptyMetrics(md); len(bad) > 0 {
			t.Errorf("exported metrics with no data points: %v", bad)
		}
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			rm := rms.At(i)
			id := ""
			if v, ok := rm.Resource().Attributes().Get("container.id"); ok {
				id = v.Str()
			}
			if id == hexID(1) {
				sawTalking = true
			}
			if id == hexID(2) {
				t.Errorf("the silent container shipped a resource with nothing measured in it")
			}
			sms := rm.ScopeMetrics()
			if sms.Len() == 0 {
				t.Errorf("resource %q shipped with no scopes at all", id)
			}
			for j := 0; j < sms.Len(); j++ {
				if sms.At(j).Metrics().Len() == 0 {
					t.Errorf("resource %q scope %d shipped carrying no metrics", id, j)
				}
			}
		}
	}
	if !sawTalking {
		t.Error("the talking container never shipped, so the assertions above ran on the wrong payload")
	}
}
