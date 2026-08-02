package servicegraph

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// logCapture collects the warnings a shard emits, so a test can assert BOTH
// that a mismatch is reported and that it is reported once.
type logCapture struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, r.Clone())
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

func (c *logCapture) messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.recs))
	for _, r := range c.recs {
		out = append(out, r.Message)
	}
	return out
}

// firstMismatchAttrs flattens the attributes of the first captured MISMATCH
// report (the processor also logs a debug line at construction).
func (c *logCapture) firstMismatchAttrs(t *testing.T) map[string]string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.recs {
		if !strings.Contains(r.Message, mismatchMarker) {
			continue
		}
		out := map[string]string{}
		r.Attrs(func(a slog.Attr) bool {
			out[a.Key] = a.Value.String()
			return true
		})
		return out
	}
	t.Fatal("no mismatch report was logged")
	return nil
}

// mismatchMarker is the substring identifying the report line. It ends in the
// colon so the SATURATION notice — which also says "mismatches" — is not
// counted as one of them.
const mismatchMarker = "config mismatch:"

func (c *logCapture) logger() *slog.Logger { return slog.New(c) }

// mismatchLines counts the captured warnings that are the mismatch report.
func (c *logCapture) mismatchLines() int {
	n := 0
	for _, m := range c.messages() {
		if strings.Contains(m, mismatchMarker) {
			n++
		}
	}
	return n
}

// claimed builds a one-span batch carrying an agent's declared trim lists, the
// way a forwarded payload does.
func claimed(dims, peers string, stamp bool) ptrace.Traces {
	td := sgTraces("checkout", clientSpan(0.1, nil))
	a := td.ResourceSpans().At(0).Resource().Attributes()
	a.PutBool(ForwardedMarker, true)
	if stamp {
		a.PutStr(ForwardedDimensions, dims)
		a.PutStr(ForwardedPeerAttributes, peers)
	}
	return td
}

// The end-to-end shape this whole mechanism exists for: what the FORWARDER
// stamps and what the PROCESSOR expects are computed from the same
// canonicalization, so a correctly configured pair reports nothing. A test
// that only checked the mismatch path could pass with two encodings that never
// agree — i.e. with the detector crying wolf on every healthy cluster.
func TestForwardedPayloadFromAMatchingAgentIsSilent(t *testing.T) {
	dims := []string{"http.route", "deployment.environment"}
	peers := []string{"peer.service", "db.name", "db.system"}
	cap := &logCapture{}
	p := NewProcessor(Config{Dimensions: dims, VirtualNodePeerAttributes: peers}, cap.logger())
	p.SetSink(&countSink{})

	// The agent's side: order deliberately differs (it is the same SET) and one
	// dimension repeats, which the trimmer tolerates and the shard dedupes.
	tr := newTrimmer([]string{"deployment.environment", "http.route", "http.route"},
		[]string{"db.system", "peer.service", "db.name"})
	td := sgTraces("checkout", clientSpan(0.1, nil))
	fwd := tr.split(td, nil, nil)[""]

	before := obs.ServiceGraphConfigMismatch.Value()
	p.Consume(fwd)
	if got := obs.ServiceGraphConfigMismatch.Value() - before; got != 0 {
		t.Errorf("mismatch counter moved by %v for an agent configured with the same sets", got)
	}
	if n := cap.mismatchLines(); n != 0 {
		t.Errorf("logged %d mismatch lines, want none: %v", n, cap.messages())
	}
}

// A disagreement is counted per forwarded RESOURCE and logged ONCE, naming both
// sides and what is lost.
func TestMismatchIsCountedAlwaysAndLoggedOnce(t *testing.T) {
	cap := &logCapture{}
	p := NewProcessor(Config{
		Dimensions:                []string{"http.route", "deployment.environment"},
		VirtualNodePeerAttributes: []string{"peer.service", "db.name"},
	}, cap.logger())
	p.SetSink(&countSink{})

	before := obs.ServiceGraphConfigMismatch.Value()
	// The agent forwards only one of the two dimensions and no db.name.
	td := claimed("http.route", "peer.service", true)
	for i := 0; i < 5; i++ {
		p.Consume(td)
	}
	if got := obs.ServiceGraphConfigMismatch.Value() - before; got != 5 {
		t.Errorf("mismatch counter moved by %v, want one per forwarded resource (5)", got)
	}
	if n := cap.mismatchLines(); n != 1 {
		t.Fatalf("logged %d mismatch lines, want exactly 1: %v", n, cap.messages())
	}
	a := cap.firstMismatchAttrs(t)
	if a["agentDimensions"] != "http.route" || a["shardDimensions"] != "deployment.environment,http.route" {
		t.Errorf("log does not name both sides: %v", a)
	}
	if a["dimensionsNotForwarded"] != "deployment.environment" {
		t.Errorf("dimensionsNotForwarded = %q, want the dimension the shard reads and the agent trims", a["dimensionsNotForwarded"])
	}
	if a["peerAttributesNotForwarded"] != "db.name" {
		t.Errorf("peerAttributesNotForwarded = %q, want db.name", a["peerAttributesNotForwarded"])
	}
}

// A DIFFERENT disagreement logs again — "once per distinct mismatch", not
// "once ever": a partially-rolled fleet has two wrong shapes and an operator
// needs to see both.
func TestMismatchLogsEachDistinctShape(t *testing.T) {
	cap := &logCapture{}
	p := NewProcessor(Config{Dimensions: []string{"http.route"}}, cap.logger())
	p.SetSink(&countSink{})

	p.Consume(claimed("", encodeAttrList(DefaultPeerAttributes()), true))
	p.Consume(claimed("", encodeAttrList(DefaultPeerAttributes()), true)) // the same shape again
	p.Consume(claimed("http.method", encodeAttrList(DefaultPeerAttributes()), true))
	if n := cap.mismatchLines(); n != 2 {
		t.Fatalf("logged %d mismatch lines, want one per distinct claim (2): %v", n, cap.messages())
	}
}

// The remembered-claims map is keyed by data off the network, so it is bounded:
// past the cap the watch says so once and stops remembering, rather than
// growing without limit or falling back to a line per batch.
func TestMismatchClaimMapIsBounded(t *testing.T) {
	cap := &logCapture{}
	p := NewProcessor(Config{Dimensions: []string{"http.route"}}, cap.logger())
	p.SetSink(&countSink{})

	const claims = maxMismatchClaims + 20
	before := obs.ServiceGraphConfigMismatch.Value()
	for i := 0; i < claims; i++ {
		p.Consume(claimed(fmt.Sprintf("attr.%d", i), "", true))
	}
	if n := len(p.mismatch.seen); n != maxMismatchClaims {
		t.Errorf("remembered %d distinct claims, want the map bounded at %d", n, maxMismatchClaims)
	}
	if n := cap.mismatchLines(); n != maxMismatchClaims {
		t.Errorf("logged %d mismatch lines, want one per remembered claim (%d)", n, maxMismatchClaims)
	}
	var saturated int
	for _, m := range cap.messages() {
		if strings.Contains(m, "too many distinct agent configurations") {
			saturated++
		}
	}
	if saturated != 1 {
		t.Errorf("saturation notice logged %d times, want exactly 1", saturated)
	}
	// Counting never stops: the log is bounded, the metric is the signal.
	if got := obs.ServiceGraphConfigMismatch.Value() - before; got != claims {
		t.Errorf("mismatch counter moved by %v, want %d — every occurrence counts", got, claims)
	}
}

// Spans that declare NOTHING are not a mismatch: they were pushed straight to
// the shard, or forwarded by an agent from before this existed. Warning on
// every batch through a rolling upgrade would make the signal worthless exactly
// when a fleet is being changed.
func TestMismatchSilentWithoutAClaim(t *testing.T) {
	cap := &logCapture{}
	p := NewProcessor(Config{Dimensions: []string{"http.route"}}, cap.logger())
	p.SetSink(&countSink{})

	before := obs.ServiceGraphConfigMismatch.Value()
	p.Consume(claimed("", "", false))                     // forwarded, but declaring nothing
	p.Consume(sgTraces("checkout", clientSpan(0.1, nil))) // not forwarded at all
	if got := obs.ServiceGraphConfigMismatch.Value() - before; got != 0 {
		t.Errorf("mismatch counter moved by %v for payloads that declare nothing", got)
	}
	if n := cap.mismatchLines(); n != 0 {
		t.Errorf("logged %d mismatch lines: %v", n, cap.messages())
	}
}

// The encoding is a SET comparison: order and repeats are not a disagreement,
// or every second cluster would warn about a list someone reordered.
func TestEncodeAttrListIsCanonical(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		want string
	}{
		{nil, ""},
		{[]string{}, ""},
		{[]string{"b", "a"}, "a,b"},
		{[]string{"a", "b"}, "a,b"},
		{[]string{"a", "a", "b", ""}, "a,b"},
		{[]string{"peer.service", "db.name", "db.system"}, "db.name,db.system,peer.service"},
	} {
		if got := encodeAttrList(tc.in); got != tc.want {
			t.Errorf("encodeAttrList(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// The claim is stamped on every forwarded resource, in the canonical encoding,
// and a nil peer list declares the DEFAULTS — which is what the agent actually
// keeps, so declaring the raw config instead would report a mismatch against a
// shard that is configured identically.
func TestForwardStampsTheAgentsEffectiveLists(t *testing.T) {
	tr := newTrimmer([]string{"http.route", "deployment.environment"}, nil)
	td := sgTraces("checkout", clientSpan(0.1, nil))
	out := tr.split(td, nil, nil)[""]
	attrs := out.ResourceSpans().At(0).Resource().Attributes()

	if v, ok := attrs.Get(ForwardedDimensions); !ok || v.Str() != "deployment.environment,http.route" {
		t.Errorf("%s = %v, want the sorted dimension list", ForwardedDimensions, v.AsString())
	}
	want := encodeAttrList(DefaultPeerAttributes())
	if v, ok := attrs.Get(ForwardedPeerAttributes); !ok || v.Str() != want {
		t.Errorf("%s = %v, want the DEFAULT peer list %q (nil config keeps the defaults)",
			ForwardedPeerAttributes, v.AsString(), want)
	}
	// Cheap by construction: one attribute pair per resource, not per span.
	spans := out.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := 0; i < spans.Len(); i++ {
		if _, ok := spans.At(i).Attributes().Get(ForwardedDimensions); ok {
			t.Errorf("the claim was stamped on a SPAN; it belongs on the resource")
		}
	}
}
