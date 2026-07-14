package promparse

import (
	"bytes"
	"fmt"
	"testing"
)

// fuzzSeedBodies are representative and adversarial exposition bodies shared
// by the parser and converter fuzz targets.
var fuzzSeedBodies = []string{
	// Representative classic exposition.
	"# HELP http_requests_total Total requests.\n" +
		"# TYPE http_requests_total counter\n" +
		"http_requests_total{code=\"200\",method=\"get\"} 1027 1395066363000\n" +
		"http_requests_total{code=\"400\",method=\"post\"} 3\n" +
		"# TYPE temp gauge\n" +
		"temp{host=\"a\"} -17.5\n",
	// Histogram + summary families.
	"# TYPE http_duration histogram\n" +
		"http_duration_bucket{le=\"0.1\"} 100\n" +
		"http_duration_bucket{le=\"0.5\"} 140\n" +
		"http_duration_bucket{le=\"+Inf\"} 144\n" +
		"http_duration_sum 53.4\n" +
		"http_duration_count 144\n" +
		"# TYPE rpc summary\n" +
		"rpc{quantile=\"0.5\"} 0.05\n" +
		"rpc{quantile=\"0.99\"} 0.9\n" +
		"rpc_sum 8000\n" +
		"rpc_count 100000\n",
	// OpenMetrics with exemplar and EOF.
	"# TYPE foo counter\n" +
		"foo_total 17.0 1520879607.789 # {trace_id=\"4bf92f3577b34da6a3ce929d0e0e4736\",span_id=\"00f067aa0ba902b7\"} 0.67 1520879607.789\n" +
		"# EOF\n",
	// Escapes, empty label block, NaN/Inf values, missing values.
	"a{b=\"c\\n\\\"d\\\\\"} 1\nempty{} 2\nnan NaN\ninf +Inf\nneg -Inf\nnoval\n",
	// TYPE redeclaration mid-exposition (converter order-key edge).
	"# TYPE x histogram\nx_bucket{le=\"1\"} 1\n# TYPE x summary\nx{quantile=\"0.5\"} 2\nx_count 3\n",
	// Buckets without le, summaries without quantile, decreasing cumulative
	// counts, duplicate le values.
	"# TYPE h histogram\nh_bucket 5\nh_bucket{le=\"2\"} 9\nh_bucket{le=\"2\"} 4\nh_bucket{le=\"1\"} 7\n# TYPE s summary\ns 3\n",
	// Malformed lines, control bytes, non-UTF8.
	"metric{a=\"unterminated\nm\x00etric 1\n\xff\xfe 2\nname{=\"v\"} 1\nname{a=} 1\nname{a=\"v\" 1\n",
	// Whitespace-heavy and comment-only.
	"   \n\t\n#\n# TYPE\n# TYPE t\n# TYPE t counter extra\n  m  1  \n",
	// Exemplar edge cases.
	"om_total 1 # {} 2\nom_total 1 #{a=\"b\"} 2 3 4\nom 2 1.5 # {a=\"b\"} 1\n",
	// Timestamp extremes.
	"m 1 9223372036854775807\nm 1 -9223372036854775808\nm 1 1e300\nm 1 0.0001\n",
}

// knownGood is a body with an exactly-known parse, used to verify a pooled
// parser is uncorrupted after parsing fuzz input.
const knownGood = "# TYPE h histogram\n" +
	"h_bucket{le=\"1\"} 1\n" +
	"h_bucket{le=\"+Inf\"} 2\n" +
	"h_sum 3\n" +
	"h_count 2\n" +
	"plain{a=\"b\"} 5\n"

type flatSample struct {
	name, family string
	role         SampleRole
	labels       string
	value        float64
}

func flatten(s Sample) flatSample {
	var lb bytes.Buffer
	for _, l := range s.Labels {
		fmt.Fprintf(&lb, "%s=%s;", l.Name, l.Value)
	}
	return flatSample{name: s.Name, family: s.Family, role: s.Role, labels: lb.String(), value: s.Value}
}

var knownGoodWant = []flatSample{
	{"h_bucket", "h", RoleHistogramBucket, "le=1;", 1},
	{"h_bucket", "h", RoleHistogramBucket, "le=+Inf;", 2},
	{"h_sum", "h", RoleHistogramSum, "", 3},
	{"h_count", "h", RoleHistogramCount, "", 2},
	{"plain", "plain", RoleGauge, "a=b;", 5},
}

// parseKnownGood runs the known-good body through a pooled parser and fails
// the test if the result deviates — the alarm for pool-state corruption.
func parseKnownGood(t *testing.T) {
	t.Helper()
	pp := Get(1<<20, false, false)
	defer Put(pp)
	var got []flatSample
	malformed, err := pp.Parse(bytes.NewReader([]byte(knownGood)), func(s Sample) error {
		got = append(got, flatten(s))
		return nil
	})
	if err != nil || malformed != 0 {
		t.Fatalf("known-good parse: malformed=%d err=%v", malformed, err)
	}
	if len(got) != len(knownGoodWant) {
		t.Fatalf("known-good parse: got %d samples, want %d: %+v", len(got), len(knownGoodWant), got)
	}
	for i := range got {
		if got[i] != knownGoodWant[i] {
			t.Fatalf("known-good sample %d: got %+v want %+v", i, got[i], knownGoodWant[i])
		}
	}
}

// FuzzParser feeds arbitrary bytes through the pooled parser path in every
// mode combination. Invariants: no panics; parse of an in-memory reader never
// errors; the malformed count stays within the physical line count; every
// emitted sample has a non-empty name and non-empty label names; the pool is
// not corrupted (a known-good body still parses exactly afterwards).
func FuzzParser(f *testing.F) {
	for _, body := range fuzzSeedBodies {
		for mode := byte(0); mode < 8; mode++ {
			f.Add([]byte(body), mode)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte, mode byte) {
		openMetrics := mode&1 != 0
		exemplars := mode&2 != 0
		maxLine := 1 << 20
		if mode&4 != 0 {
			maxLine = 100 // exercise the too-long-line path, incl. bufio spill
		}

		pp := Get(maxLine, openMetrics, exemplars)
		samples := 0
		malformed, err := pp.Parse(bytes.NewReader(data), func(s Sample) error {
			samples++
			if s.Name == "" {
				t.Errorf("sample %d: empty name", samples)
			}
			if s.Family == "" {
				t.Errorf("sample %d (%q): empty family", samples, s.Name)
			}
			for _, l := range s.Labels {
				if l.Name == "" {
					t.Errorf("sample %q: empty label name", s.Name)
				}
			}
			if s.Exemplar != nil {
				if !exemplars {
					t.Errorf("sample %q: exemplar emitted with exemplars disabled", s.Name)
				}
				for _, l := range s.Exemplar.Labels {
					if l.Name == "" {
						t.Errorf("sample %q: empty exemplar label name", s.Name)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("parse returned error for in-memory input: %v", err)
		}
		lines := bytes.Count(data, []byte{'\n'}) + 1
		if malformed < 0 || malformed > lines {
			t.Fatalf("malformed=%d out of range (lines=%d)", malformed, lines)
		}
		if samples+malformed > lines {
			t.Fatalf("samples=%d + malformed=%d exceeds physical lines %d", samples, malformed, lines)
		}
		Put(pp)

		// The recycled parser must be uncorrupted.
		parseKnownGood(t)
	})
}
