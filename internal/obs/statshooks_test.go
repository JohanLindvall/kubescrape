package obs

import "testing"

// Every struct-shaped Register*Stats hook turns ONE snapshot closure into one
// func registration per field, and the Registry evaluates them back to back in
// a single pass. Unmemoised, each field called the snapshot again — the
// source's lock taken once per field per export, and the fields published as
// readings from different instants. The memo (metrics.PerPass) used to exist at
// one caller only (the service-graph wiring in cmd/kubescrape-agent); it now
// sits inside the hooks, so every caller gets it.
//
// This registers into the process-global Registry, like the hooks themselves:
// the series are func-backed and read nothing but the fakes below, so a repeat
// run (-count=N) only adds another func beside the first one's.
func TestStatsHooksSampleTheirSourceOncePerPass(t *testing.T) {
	var sg, ts, st, buf int
	RegisterServiceGraphStats(func() ServiceGraphStat {
		sg++
		return ServiceGraphStat{Pending: sg, Completed: uint64(sg), VirtualNode: uint64(sg), Unkeyable: uint64(sg)}
	})
	RegisterTailSamplingStats(func() TailSamplingStat {
		ts++
		return TailSamplingStat{Traces: ts, Spans: ts}
	})
	RegisterStoreStats(func() (int, int) {
		st++
		return st, st
	})
	RegisterBufferStats(func() map[string]BufferStat {
		buf++
		return map[string]BufferStat{"logs": {Backlog: int64(buf), Cap: int64(buf), Segments: buf}}
	})

	for pass := 1; pass <= 2; pass++ {
		Registry.Dump()
		for name, got := range map[string]int{
			"RegisterServiceGraphStats": sg,
			"RegisterTailSamplingStats": ts,
			"RegisterStoreStats":        st,
			"RegisterBufferStats":       buf,
		} {
			if got != pass {
				t.Errorf("after %d scrape(s), %s sampled its source %d times, want %d: once per pass, not once per field",
					pass, name, got, pass)
			}
		}
	}
}
