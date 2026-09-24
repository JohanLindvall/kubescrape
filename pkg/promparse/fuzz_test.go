package promparse

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

// fuzzSeedsFile holds the exposition seed bodies, one strconv.Quote'd string
// per line. It is SHARED with internal/agent/promscrape's FuzzConverter, which
// reads this same file, so a seed added for a parser bug reaches the converter
// too — the two used to carry byte-identical copies that nothing kept in step.
const fuzzSeedsFile = "testdata/fuzzseeds.txt"

// loadFuzzSeeds reads a seed file in fuzzseeds.txt's format (see its header).
// internal/agent/promscrape's fuzz_test.go carries the same few lines: test
// helpers cannot cross a package boundary without an exported test package,
// and this one would put fixtures on a public import surface.
func loadFuzzSeeds(tb testing.TB, path string) []string {
	tb.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	var seeds []string
	for n, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		s, err := strconv.Unquote(line)
		if err != nil {
			tb.Fatalf("%s:%d: %v", path, n+1, err)
		}
		seeds = append(seeds, s)
	}
	if len(seeds) == 0 {
		tb.Fatalf("%s holds no seeds", path)
	}
	return seeds
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
	pp := Get(Options{MaxLineBytes: 1 << 20})
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
// errors; the malformed count stays within the physical line count; the
// MalformedDetail breakdown never sums past the malformed total it attributes;
// no exemplar is emitted OR counted malformed unless exemplar parsing is on
// (OpenMetrics and Exemplars both); every emitted sample has a non-empty name
// and non-empty label names; each byte-bounded table (TYPE, HELP/UNIT, the
// interned names) is charged EXACTLY what it retains and stays within its
// budget — a charge that drifts from the table either refuses families it has
// room for or stops bounding memory, and nothing else would notice; the pool is
// not corrupted (a known-good body still parses exactly afterwards).
func FuzzParser(f *testing.F) {
	for _, body := range loadFuzzSeeds(f, fuzzSeedsFile) {
		for mode := range byte(8) {
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

		pp := Get(Options{MaxLineBytes: maxLine, OpenMetrics: openMetrics, Exemplars: exemplars})
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
				if !openMetrics || !exemplars {
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
		// Read before Put, as the accessors' docs require.
		if d := pp.MalformedDetail(); d.OverLongLines+d.TruncatedLines+d.DuplicateLabels+d.TooManyLabels > malformed {
			t.Fatalf("MalformedDetail %+v sums past malformed=%d: it attributes the total and must never exceed it", d, malformed)
		}
		if bad := pp.MalformedExemplars(); bad != 0 && (!openMetrics || !exemplars) {
			t.Fatalf("MalformedExemplars=%d with exemplar parsing off (openMetrics=%v exemplars=%v)", bad, openMetrics, exemplars)
		}
		checkTableCharges(t, pp.p)
		Put(pp)

		// The recycled parser must be uncorrupted.
		parseKnownGood(t)
	})
}

// checkTableCharges asserts that every byte-bounded table is charged exactly
// what it holds, counted off the table itself, and holds no more than its
// budget. The name table is warm across pooled parses, so its identity holds
// at every point, not just after a fresh parser's first parse.
func checkTableCharges(t *testing.T, p *Parser) {
	t.Helper()
	if got := retainedTypeBytes(p); got != p.typeBytes || got > maxTypeBytes {
		t.Fatalf("TYPE table retains %d bytes, charged %d, budget %d", got, p.typeBytes, maxTypeBytes)
	}
	meta := 0
	for k, m := range p.metas {
		meta += len(k) + len(m.help) + len(m.unit)
	}
	if meta != p.metaBytes || meta > maxMetaBytes {
		t.Fatalf("HELP/UNIT table retains %d bytes, charged %d, budget %d", meta, p.metaBytes, maxMetaBytes)
	}
	if got := retainedNameBytes(p); got != p.nameBytes || got > maxInternedNameBytes {
		t.Fatalf("name intern table retains %d bytes, charged %d, budget %d", got, p.nameBytes, maxInternedNameBytes)
	}
}
