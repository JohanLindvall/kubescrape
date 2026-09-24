package debugtap

// A stream with no filter and no sampling — the default GET /debug/otlp and
// the UI's — kept every resource and still deep-copied the whole payload
// before marshalling the copy, on the EXPORTING goroutine: ~70% of the render's
// allocations and bytes bought nothing, since the JSON marshaler only reads
// and offer renders while the exporter's caller still holds the payload.

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

func TestUnfilteredStreamRendersThePayloadWithoutCopyingIt(t *testing.T) {
	tap := New(&fakeInner{})
	all := &subscriber{signals: sigAll, sample: 100}
	ld := benchLogs(64, 16)

	// Byte-identical to marshalling the payload itself: the shortcut changes
	// what it costs, never what the stream shows.
	got, fail := tap.renderLogs(ld, all)
	want, err := logsMarshaler.MarshalLogs(ld)
	if fail != nil || err != nil {
		t.Fatalf("render failed: %v / %v", fail, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the unfiltered render differs from the payload's own rendering")
	}

	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(7)
	if got, _ := tap.renderMetrics(md, all); !bytes.Equal(got, mustMarshal(metricsMarshaler.MarshalMetrics(md))) {
		t.Error("the unfiltered metrics render differs from the payload's own rendering")
	}
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("s")
	if got, _ := tap.renderTraces(td, all); !bytes.Equal(got, mustMarshal(tracesMarshaler.MarshalTraces(td))) {
		t.Error("the unfiltered traces render differs from the payload's own rendering")
	}
	// An empty payload still renders to nothing.
	if got, fail := tap.renderMetrics(pmetric.NewMetrics(), all); got != nil || fail != nil {
		t.Errorf("an empty payload rendered to (%q, %v), want nothing", got, fail)
	}

	if testrace.Enabled {
		return // the detector changes escape analysis and adds bookkeeping allocations
	}
	// And it costs what the marshal costs, with no copy in front of it.
	marshal := testing.AllocsPerRun(5, func() { _, _ = logsMarshaler.MarshalLogs(ld) })
	render := testing.AllocsPerRun(5, func() { _, _ = tap.renderLogs(ld, all) })
	if render > marshal+2 {
		t.Errorf("an unfiltered render costs %.0f allocations against the marshal's %.0f: the payload is being copied first", render, marshal)
	}
}

func mustMarshal(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}
