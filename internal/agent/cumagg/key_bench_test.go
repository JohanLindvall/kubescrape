package cumagg

import (
	"strconv"
	"testing"
)

// The series key is rebuilt for EVERY span and every completed edge, on the
// trace tier's receive path, and then hashed and compared by the map lookup it
// feeds — profiling agent/spanmetrics' fold at the 20000-series cap put key
// construction plus that lookup at about half the per-span cost. So the
// encoding is worth measuring on its own: it is not a formatting detail, it is
// two of the four things a span costs.
//
// The shapes are the two aggregators' real ones: spanmetrics keys
// (service, span name, kind, status), servicegraph keys (client, server,
// connection_type, virtual_node) plus a pair per configured dimension.
var (
	spanKeyParts  = []string{"checkout", "GET /api/v2/orders/{orderId}/shipments", "SPAN_KIND_SERVER", "STATUS_CODE_OK"}
	edgeKeyParts  = []string{"checkout", "orders", "database", "", "client_http.method", "GET", "server_db.system", "postgresql"}
	longKeyParts  = []string{"checkout", string(make([]byte, 200)), "SPAN_KIND_CLIENT", "STATUS_CODE_ERROR"}
	keyPartsCases = []struct {
		name  string
		parts []string
	}{{"span-metrics", spanKeyParts}, {"service-graph", edgeKeyParts}, {"long-value", longKeyParts}}
)

func BenchmarkAppendKeyPart(b *testing.B) {
	for _, tc := range keyPartsCases {
		b.Run(tc.name, func(b *testing.B) {
			var scratch [512]byte
			b.ReportAllocs()
			for b.Loop() {
				key := scratch[:0]
				for _, p := range tc.parts {
					key = AppendKeyPart(key, p)
				}
				sink = len(key)
			}
		})
	}
}

var sink int

// The prefix's only job is to make the concatenation injective: ("a","bc") and
// ("ab","c") must not key the same. Nothing ever reads it back, so the encoding
// is free to be whatever is cheapest — but it MUST stay injective, and the
// value that proves it is the pair a plain concatenation collides on.
func TestKeyPartsAreInjective(t *testing.T) {
	seen := map[string][]string{}
	for _, parts := range [][]string{
		{"a", "bc"}, {"ab", "c"}, {"", "abc"}, {"abc", ""}, {"abc"},
		{"a", "b", "c"}, {"ab", ""}, {"", "ab"},
		// The length boundaries the encoding switches at.
		{strings127, "x"}, {strings128, "x"}, {strings129, "x"},
		{strings127 + "x"}, {strings128 + "x"},
	} {
		var buf []byte
		for _, p := range parts {
			buf = AppendKeyPart(buf, p)
		}
		k := string(buf)
		if prev, ok := seen[k]; ok {
			t.Errorf("%q and %q build the same key", prev, parts)
		}
		seen[k] = parts
	}
}

var (
	strings127 = strconv.Itoa(0) + string(make([]byte, 126))
	strings128 = strconv.Itoa(0) + string(make([]byte, 127))
	strings129 = strconv.Itoa(0) + string(make([]byte, 128))
)
