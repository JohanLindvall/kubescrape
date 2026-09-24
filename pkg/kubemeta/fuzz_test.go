package kubemeta

import (
	"encoding/binary"
	"errors"
	"maps"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// FuzzNormalizeContainerID checks NormalizeContainerID never panics and is
// idempotent: normalizing an already-normalized ID must be a no-op, since the
// server normalizes IDs arriving both raw and already-stripped (agent paths,
// HTTP path cleaning).
func FuzzNormalizeContainerID(f *testing.F) {
	for _, s := range []string{
		"containerd://abcdef0123456789",
		"docker://abc",
		"cri-o://abc",
		"containerd:/abc", // collapsed by HTTP path cleaning
		"abc",
		"",
		"   containerd://abc   ",
		"://",
		":",
		"a:b",
		"scheme://a:b",
		"containerd:// abc",
		"containerd:///abc",
		"\x00:\xff//x",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, id string) {
		once := NormalizeContainerID(id)
		twice := NormalizeContainerID(once)
		if once != twice {
			t.Fatalf("NormalizeContainerID not idempotent: %q -> %q -> %q", id, once, twice)
		}
	})
}

// encodeAnnotations is FuzzFilterAnnotations' input format, used to build its
// seeds: per pair, a 1-byte key length, the key, a 2-byte big-endian value
// length, and ONE fill byte the value repeats — so a corpus entry of a few
// bytes can still carry a value past MaxAnnotationValueBytes.
func encodeAnnotations(pairs ...string) []byte {
	var b []byte
	for i := 0; i+1 < len(pairs); i += 2 {
		k, v := pairs[i], pairs[i+1]
		b = append(b, byte(len(k)))
		b = append(b, k...)
		b = binary.BigEndian.AppendUint16(b, uint16(len(v)))
		fill := byte('v')
		if len(v) > 0 {
			fill = v[0]
		}
		b = append(b, fill)
	}
	return b
}

// decodeAnnotations is encodeAnnotations' inverse over ARBITRARY bytes: a
// truncated trailing pair is dropped, and a repeated key keeps its last value.
func decodeAnnotations(data []byte) map[string]string {
	m := map[string]string{}
	for len(data) > 0 {
		kl := int(data[0])
		data = data[1:]
		if len(data) < kl+3 {
			break
		}
		k := string(data[:kl])
		vl := int(binary.BigEndian.Uint16(data[kl : kl+2]))
		m[k] = strings.Repeat(string(data[kl+2:kl+3]), vl)
		data = data[kl+3:]
	}
	return m
}

// FuzzFilterAnnotations checks the one door every tenant-authored annotation
// set passes on its way to the unauthenticated routes. The filter has a
// length-checked fast path and an ordered slow path that must agree, and
// nothing but fixed maps exercised either: this mixes refused keys, a forged
// note, oversized values and budget pressure, and asserts what the served map
// must satisfy whatever arrives —
//
//   - every served value is within MaxAnnotationValueBytes, and the served set
//     (the note aside) within MaxAnnotationBytes;
//   - no refused key (the deploy-tool copies, the reserved notes) is served,
//     except the note this filter wrote itself;
//   - everything served was in the input, with its input value;
//   - the note is present exactly when some non-refused input key is missing,
//     and bounded by its prose plus maxOmittedNamedBytes plus a count;
//   - the result is deterministic, nil rather than empty, and equal to
//     filterAnnotationsDefinition's.
func FuzzFilterAnnotations(f *testing.F) {
	kib := strings.Repeat("x", 1<<10)
	var many []string
	for i := range 40 {
		many = append(many, "team.example.com/"+strconv.Itoa(i), kib)
	}
	for _, seed := range [][]byte{
		encodeAnnotations("app", "web", "prometheus.io/scrape", "true"),
		encodeAnnotations("kubectl.kubernetes.io/last-applied-configuration", "{}", "app", "web"),
		encodeAnnotations("kapp.k14s.io/original", "{}"),
		encodeAnnotations(OmittedAnnotation, "forged", LabelsOmittedAnnotation, "forged"),
		encodeAnnotations("team.example.com/blob", strings.Repeat("b", MaxAnnotationValueBytes+1), "app", "web"),
		encodeAnnotations("team.example.com/edge", strings.Repeat("e", MaxAnnotationValueBytes)),
		encodeAnnotations(many...),
		encodeAnnotations(append([]string{
			"prometheus.io/scrape", "true", "prometheus.io/port", "9090", "kubescrape.io/logs", "{}",
			"z/blob", strings.Repeat("z", MaxAnnotationValueBytes), OmittedAnnotation, "forged",
		}, many...)...),
		nil,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		in := decodeAnnotations(data)
		got := FilterAnnotations(in)
		if got != nil && len(got) == 0 {
			t.Fatal("an empty result must be nil, so the field stays omitempty")
		}
		if again := FilterAnnotations(in); !maps.Equal(got, again) {
			t.Fatal("two calls on one input disagree: the served document would mint a fresh ETag per poll")
		}
		if want := filterAnnotationsDefinition(in); !maps.Equal(got, want) {
			t.Fatalf("FilterAnnotations disagrees with its definition: got %d keys, want %d", len(got), len(want))
		}
		total := 0
		for k, v := range got {
			if k == OmittedAnnotation {
				continue
			}
			if refusedAnnotations[k] {
				t.Fatalf("refused key %q was served", k)
			}
			if iv, ok := in[k]; !ok || iv != v {
				t.Fatalf("served %q, which is not the input's value for it", k)
			}
			if len(v) > MaxAnnotationValueBytes {
				t.Fatalf("%q served at %d bytes, over the value ceiling", k, len(v))
			}
			total += len(k) + len(v)
		}
		if total > MaxAnnotationBytes {
			t.Fatalf("served set is %d bytes, over the %d-byte budget", total, MaxAnnotationBytes)
		}
		refused := 0
		for k := range in {
			if _, ok := got[k]; !ok && !refusedAnnotations[k] {
				refused++
			}
		}
		note, noted := got[OmittedAnnotation]
		if noted != (refused > 0) {
			t.Fatalf("note present = %v, but %d non-refused input keys are missing", noted, refused)
		}
		if !noted {
			return
		}
		if !strings.HasPrefix(note, strconv.Itoa(refused)+" annotation(s) omitted") {
			t.Fatalf("the note does not count the %d refused keys: %.80q", refused, note)
		}
		digits := len(strconv.Itoa(refused))
		if bound := len(omittedNote(nil)) - 1 + digits + len("; omitted: ") + maxOmittedNamedBytes +
			len(", +") + digits + len(" more"); len(note) > bound {
			t.Fatalf("the note is %d bytes, over its %d-byte bound", len(note), bound)
		}
	})
}

// FuzzRelabelRegexCostAgreesWithCompile holds the three properties the parse
// door and the agent rely on: RelabelRegexCost and regexp.Compile agree on
// whether a string is a regex (so the door never admits a rule the agent
// cannot compile, nor refuses one it can); its cost scan tokenises every regex
// the parser accepts to the END (a scan that stops short stops charging, which
// is the one way it could under-charge); and the cost it admits upper-bounds
// the program the compiler builds.
func FuzzRelabelRegexCostAgreesWithCompile(f *testing.F) {
	for _, seed := range []string{
		"", "a)|(b", `(?i)[B-\x{1E942}]`, `[\pL\p{Greek}[:alpha:]\d-]`, `\QA[B\E`, `[\x{41}-\101]`,
		"(?:abc){999}", `[]a-]`, `[^\]\\]`, `(?P<n>x)\1`, "\xff",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, regex string) {
		cost, err := RelabelRegexCost(regex)
		if errors.Is(err, ErrRelabelRegexTooLarge) {
			return // refused on cost, and compiling it here is the cost refused
		}
		src := anchoredRelabelRegex(regex)
		_, compileErr := regexp.Compile(src)
		switch {
		case (err == nil) != (compileErr == nil):
			t.Fatalf("RelabelRegexCost error %v, regexp.Compile error %v", err, compileErr)
		case err != nil:
			return
		}
		if scanned, complete := parseCost(src, MaxRelabelRegexCost); !complete {
			t.Fatalf("the cost scan stopped short (cost %d) on a regex the parser accepts", scanned)
		}
		if n := compiledInsts(t, regex); n > cost+2 {
			t.Fatalf("compiles to %d instructions, but cost %d was admitted", n, cost)
		}
	})
}
