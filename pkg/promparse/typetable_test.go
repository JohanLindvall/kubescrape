package promparse

import (
	"strings"
	"testing"
)

// retainedTypeBytes is what the TYPE table actually holds alive: the parser is
// still ours after Parse, so this is the heap a scrape left behind, measured
// the way a heap profile would — not the parser's own accounting, which a
// broken charge could make agree with itself.
func retainedTypeBytes(p *Parser) int {
	n := 0
	for k := range p.types {
		n += len(k)
	}
	return n
}

// A count cap is not a memory cap: the TYPE table's KEYS are names the target
// chooses. The reported production shape is one annotated pod serving a ~2 MiB
// gzip body of 100k `# TYPE <16 KiB name> counter` lines and no samples at all,
// which retained >1.5 GB of live heap in the node's agent — an OOMKill reported
// as a scrape with outcome "ok". Scaled down here to 4 MB of attempted
// retention, which is still 4x the budget and cheap to run.
func TestTypeTableRetentionIsBoundedByBytes(t *testing.T) {
	const (
		names   = 4000
		nameLen = 1000 // under maxFamilyNameBytes: the per-token cap must not be what saves us
	)
	var sb strings.Builder
	for i := 0; i < names; i++ {
		sb.WriteString("# TYPE ")
		sb.WriteString(strings.Repeat("a", nameLen-8))
		sb.WriteString(pad8(i))
		sb.WriteString(" counter\n")
	}
	// One early family also carries a sample, so the test can tell a bound that
	// degrades (families past the budget lose their type) from one that breaks
	// (nothing is typed at all).
	first := strings.Repeat("a", nameLen-8) + pad8(0)
	sb.WriteString(first + " 1\n")

	p := New(Options{})
	var got []Sample
	malformed, err := p.Parse(strings.NewReader(sb.String()), func(s Sample) error {
		got = append(got, s)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if malformed != 0 {
		t.Fatalf("malformed = %d, want 0: every line here is a valid TYPE declaration", malformed)
	}
	if retained := retainedTypeBytes(p); retained > maxTypeBytes {
		t.Fatalf("TYPE table retains %d bytes of family names after the parse, want <= %d "+
			"(%d declarations of %d bytes: a target chooses these keys, so bounding the entry "+
			"COUNT alone leaves MaxTrackedFamilies x MaxLineBytes of reachable heap)",
			retained, maxTypeBytes, names, nameLen)
	}
	if n := len(p.types); n < 100 {
		t.Fatalf("only %d families typed: the byte bound must DEGRADE (later families go untyped), not refuse the exposition", n)
	}
	if len(got) != 1 || got[0].Role != RoleCounter {
		t.Fatalf("sample of a family declared before the budget ran out: %+v, want one RoleCounter sample", got)
	}
}

// pad8 renders a fixed-width suffix so every generated family name is exactly
// nameLen bytes (a varying length would make the retention arithmetic fuzzy).
func pad8(i int) string {
	const digits = "0123456789"
	var b [8]byte
	for j := 7; j >= 0; j-- {
		b[j] = digits[i%10]
		i /= 10
	}
	return string(b[:])
}

// A single absurd family token must not be able to spend the whole table
// budget, so it is refused before it is retained — and refused LOUDLY, on the
// malformed path a broken TYPE line already takes, because a silent cap here
// would leave every later family of the exposition untyped with nothing moving.
func TestOverLongFamilyTokenIsRefusedAndCounted(t *testing.T) {
	long := strings.Repeat("f", maxFamilyNameBytes+1)
	in := "# TYPE " + long + " counter\n" + long + " 7\n"

	p := New(Options{})
	var got []Sample
	malformed, err := p.Parse(strings.NewReader(in), func(s Sample) error {
		got = append(got, s)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if malformed != 1 {
		t.Fatalf("malformed = %d, want 1: an over-long TYPE family is refused and counted, not silently dropped", malformed)
	}
	if _, ok := p.types[long]; ok {
		t.Fatalf("the refused family was retained in the TYPE table anyway (%d bytes)", len(long))
	}
	if retained := retainedTypeBytes(p); retained != 0 {
		t.Fatalf("TYPE table retains %d bytes after a refused declaration, want 0", retained)
	}
	// The refusal costs the family its TYPE, never its data.
	if len(got) != 1 || got[0].Value != 7 || got[0].Role != RoleGauge {
		t.Fatalf("sample of the refused family: %+v, want it emitted once as an untyped gauge", got)
	}
}

// The boundary, spelled out: maxFamilyNameBytes is accepted and one byte more
// is not. An off-by-one here is a silently untyped family on one side and a
// counted-malformed valid line on the other.
func TestFamilyTokenBoundary(t *testing.T) {
	for _, tc := range []struct {
		n             int
		wantTyped     bool
		wantMalformed int
	}{
		{maxFamilyNameBytes, true, 0},
		{maxFamilyNameBytes + 1, false, 1},
	} {
		name := strings.Repeat("g", tc.n)
		p := New(Options{})
		malformed, err := p.Parse(strings.NewReader("# TYPE "+name+" counter\n"), func(Sample) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if _, typed := p.types[name]; typed != tc.wantTyped {
			t.Fatalf("family of %d bytes: typed = %v, want %v", tc.n, typed, tc.wantTyped)
		}
		if malformed != tc.wantMalformed {
			t.Fatalf("family of %d bytes: malformed = %d, want %d", tc.n, malformed, tc.wantMalformed)
		}
	}
}

// The quoted (Prometheus 3 UTF-8) form goes through the same token, so it must
// take the same bound — otherwise the cap is one quote character away from
// being no cap at all.
func TestQuotedFamilyTokenTakesTheSameBound(t *testing.T) {
	long := strings.Repeat("q", maxFamilyNameBytes+1)
	p := New(Options{})
	malformed, err := p.Parse(strings.NewReader("# TYPE \""+long+"\" counter\n"), func(Sample) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if malformed != 1 || len(p.types) != 0 {
		t.Fatalf("quoted over-long family: malformed = %d, %d families tracked, want 1 and 0", malformed, len(p.types))
	}
}

// A family whose TYPE is repeated — exporters that re-declare it before every
// sample exist — must be charged ONCE. Charging per line would spend the whole
// budget on one legitimate family and leave the rest of the exposition untyped.
func TestRepeatedTypeDeclarationIsChargedOnce(t *testing.T) {
	const repeats = 2000
	name := strings.Repeat("r", 1000) // 2 MB if every repeat were charged
	var sb strings.Builder
	for i := 0; i < repeats; i++ {
		sb.WriteString("# TYPE " + name + " counter\n")
	}
	sb.WriteString("# TYPE later_family counter\nlater_family 1\n")

	p := New(Options{})
	var got []Sample
	if _, err := p.Parse(strings.NewReader(sb.String()), func(s Sample) error {
		got = append(got, s)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if p.typeBytes > 2*len(name) {
		t.Fatalf("typeBytes = %d after %d repeats of one %d-byte family, want ~%d: the charge is per KEY, not per line",
			p.typeBytes, repeats, len(name), len(name))
	}
	if len(got) != 1 || got[0].Role != RoleCounter {
		t.Fatalf("the family declared after the repeats: %+v, want one RoleCounter sample (its TYPE was refused by a spent budget)", got)
	}
}

// The budget is per EXPOSITION, like the table it bounds. A charge left
// standing over a cleared table would silently untype the next scrape's
// families — on both entry points, since the pooled path and Parse clear the
// table separately.
func TestTypeBudgetResetsBetweenExpositions(t *testing.T) {
	// Names of exactly maxFamilyNameBytes, so the budget is spent to the byte and
	// even an 11-byte family cannot be admitted afterwards — a leak of any size
	// would otherwise hide in the headroom a rounder number leaves behind.
	var sb strings.Builder
	for i := 0; i < maxTypeBytes/maxFamilyNameBytes+16; i++ {
		sb.WriteString("# TYPE " + strings.Repeat("a", maxFamilyNameBytes-8) + pad8(i) + " counter\n")
	}
	exhaust, second := sb.String(), "# TYPE fresh_total counter\nfresh_total 1\n"

	t.Run("Parse", func(t *testing.T) {
		p := New(Options{})
		if _, err := p.Parse(strings.NewReader(exhaust), func(Sample) error { return nil }); err != nil {
			t.Fatal(err)
		}
		var got []Sample
		if _, err := p.Parse(strings.NewReader(second), func(s Sample) error {
			got = append(got, s)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Role != RoleCounter {
			t.Fatalf("second exposition: %+v, want one RoleCounter sample (a leaked byte charge untypes it)", got)
		}
	})

	// The pooled entry point clears the table in its own place, so it gets its
	// own round trip — Put followed by Get hands back the same *Pooled with high
	// probability, which is how TestAudit_PooledReuseNoCrossContamination pins
	// the sibling leaks through this path.
	t.Run("Pooled", func(t *testing.T) {
		pp := Get(Options{})
		if _, err := pp.Parse(strings.NewReader(exhaust), func(Sample) error { return nil }); err != nil {
			t.Fatal(err)
		}
		Put(pp)
		pp = Get(Options{})
		defer Put(pp)
		var got []Sample
		if _, err := pp.Parse(strings.NewReader(second), func(s Sample) error {
			got = append(got, s)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Role != RoleCounter {
			t.Fatalf("second exposition on a pooled parser: %+v, want one RoleCounter sample", got)
		}
	})
}
