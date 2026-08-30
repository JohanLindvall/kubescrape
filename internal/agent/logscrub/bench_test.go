package logscrub

import "testing"

// The scrubber runs on every exported record of every log producer, on the
// tailer's SINGLE sweep goroutine, and the no-match path is the one that runs
// on essentially all of them. BenchmarkScrubNoMatch (logscrub_test.go) measures
// it on an 88-byte plain line; these add the shapes a real node actually
// carries, because the prefilters' cost is a function of the LINE, not of the
// pattern set:
//
//   - a structured JSON application line, 4x longer, which is what the
//     per-byte scans (containsFold's fold scan and secretKVCandidate's
//     dispatch pass) actually pay for;
//   - a line containing '@', which is not exotic — every image reference
//     (`image@sha256:…`), every email and every `user@host` carries one, and
//     it is the one default prefilter that admits on a single byte, so these
//     lines pay a full url-userinfo regex pass and find nothing;
//   - a line that is redacted, so the replacement path has a number too.
const (
	plainNoMatch = `2026-07-24T10:00:00Z INFO handled request path=/api/v1/resource status=200 duration=12ms`

	jsonNoMatch = `{"level":"info","ts":"2026-07-24T10:00:00.123456Z","logger":"http.server",` +
		`"msg":"handled request","path":"/api/v1/orders","method":"GET","status":200,` +
		`"duration_ms":42.5,"trace_id":"0af7651916cd43dd8448eb211c80319c","user_id":"u-90210",` +
		`"pod":"payments-6f7b9c001","namespace":"prod-payments"}`

	atNoMatch = `2026-07-24T10:00:00Z INFO pulled image ghcr.io/acme/payments` +
		`@sha256:9f2c4b1e7a3d5c8f0b6e2a4d7c9f1b3e5a7c9d1f3b5e7a9c1d3f5b7e9a1c3d5f for pod payments-6f7b9c001`

	kvMatch = `2026-07-24T10:00:00Z WARN dial failed for db-1 password=hunter2 retrying in 5s`
)

func benchScrub(b *testing.B, line string, wantChanged bool) {
	s, err := New(Config{Builtin: []string{"defaults"}})
	if err != nil {
		b.Fatal(err)
	}
	if changed := s.Scrub(line) != line; changed != wantChanged {
		b.Fatalf("Scrub changed = %v, want %v", changed, wantChanged)
	}
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Scrub(line)
	}
}

func BenchmarkScrubNoMatchJSON(b *testing.B)  { benchScrub(b, jsonNoMatch, false) }
func BenchmarkScrubNoMatchPlain(b *testing.B) { benchScrub(b, plainNoMatch, false) }
func BenchmarkScrubNoMatchAt(b *testing.B)    { benchScrub(b, atNoMatch, false) }
func BenchmarkScrubMatch(b *testing.B)        { benchScrub(b, kvMatch, true) }
