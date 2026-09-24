package promparse

import (
	"strconv"
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

// retainedNameBytes is what the name intern table holds alive, counted off the
// table rather than off nameBytes.
func retainedNameBytes(p *Parser) int {
	n := 0
	for k := range p.names {
		n += len(k)
	}
	return n
}

// The name intern table had a count cap (MaxTrackedFamilies) and a per-entry
// length cap (256 B) and no byte budget, so one scrape of 99,990 distinct
// 256-byte metric names left 29.5 MiB retained — and a POOLED parser keeps the
// table warm on purpose, clearing it only past half a bound, so up to ~15 MiB
// of one target's names could sit in every pooled parser between scrapes. Every
// other per-exposition table here is held to 1 MiB; so is this one now, and a
// recycled parser keeps at most half of that.
func TestNameInternTableIsBoundedByBytes(t *testing.T) {
	const (
		names   = 20_000
		nameLen = maxInternedNameLen // the longest a name may be and still intern
	)
	var sb strings.Builder
	for i := range names {
		sb.WriteString(strings.Repeat("n", nameLen-8))
		sb.WriteString(pad8(i))
		sb.WriteString(" 1\n")
	}
	pp := Get(Options{})
	defer Put(pp)
	n := 0
	if _, err := pp.Parse(strings.NewReader(sb.String()), func(s Sample) error {
		// Past the budget a name is allocated normally, never lost.
		if want := strings.Repeat("n", nameLen-8) + pad8(n); s.Name != want {
			t.Fatalf("sample %d is named %.12s..., want the name it was written with", n, s.Name)
		}
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != names {
		t.Fatalf("emitted %d samples, want %d", n, names)
	}
	if got := retainedNameBytes(pp.p); got > maxInternedNameBytes || got != pp.p.nameBytes {
		t.Fatalf("name table retains %d bytes (charged %d) after %d distinct %d-byte names, want <= %d",
			got, pp.p.nameBytes, names, nameLen, maxInternedNameBytes)
	}
	if len(pp.p.names) < 100 {
		t.Fatalf("only %d names interned: the byte bound must stop the table GROWING, not disable it", len(pp.p.names))
	}

	// What a recycled parser carries into its next scrape: the pool round trip,
	// on this same parser (see Pooled.reset).
	pp.release()
	pp.reset(Options{})
	if got := retainedNameBytes(pp.p); got >= maxInternedNameBytes/2 || got != pp.p.nameBytes {
		t.Fatalf("a recycled parser keeps %d bytes of names (charged %d), want < %d",
			got, pp.p.nameBytes, maxInternedNameBytes/2)
	}
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
	for i := range names {
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
	for range repeats {
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

// An UNCHANGED redeclaration — the exporters that repeat TYPE before every
// sample repeat HELP too — changes no classification, so it must not invalidate
// the per-name memo that spares every sample classify's map probe and suffix
// walks. A real type change still must, or the family's later samples keep the
// role they had under the old type.
func TestUnchangedTypeRedeclarationKeepsTheClassifyMemo(t *testing.T) {
	p := New(Options{})
	body := "# TYPE x counter\n# HELP x h\nx_total 1\n# TYPE x counter\n# HELP x h\n"
	if _, err := p.Parse(strings.NewReader(body), func(Sample) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !p.lastClassOK {
		t.Fatal("an unchanged TYPE+HELP redeclaration invalidated the classify memo")
	}

	var got []Sample
	body = "# TYPE y counter\ny_total 1\n# TYPE y counter\ny_total 2\n# TYPE y gauge\ny_total 3\n"
	if _, err := p.Parse(strings.NewReader(body), func(s Sample) error {
		got = append(got, s)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []struct {
		role   SampleRole
		family string
	}{{RoleCounter, "y"}, {RoleCounter, "y"}, {RoleGauge, "y_total"}}
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Role != w.role || got[i].Family != w.family {
			t.Errorf("sample %d: role=%v family=%q, want role=%v family=%q (a retyped family must reclassify)",
				i, got[i].Role, got[i].Family, w.role, w.family)
		}
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
	for i := range maxTypeBytes/maxFamilyNameBytes + 16 {
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

	// The pooled entry point gets its own round trip, on ONE parser: a Put
	// followed by a Get may hand back a fresh parser (and under -race the pool
	// drops one Put in four on purpose), which would pass vacuously.
	t.Run("Pooled", func(t *testing.T) {
		pp := Get(Options{})
		defer Put(pp)
		if _, err := pp.Parse(strings.NewReader(exhaust), func(Sample) error { return nil }); err != nil {
			t.Fatal(err)
		}
		pp.release()
		pp.reset(Options{})
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

// The HELP/UNIT budget must be charged per RETAINED family, exactly as the TYPE
// table's is charged per NEW key — not once per DECLARATION. Exporters that
// repeat a family's `# HELP` before every one of its samples exist (they are the
// same ones that repeat its `# TYPE`, which the table above already accounts
// for): at ~100 bytes a repetition the 1 MiB budget was spent after ~10k lines,
// and from there every family declared LATER in the same exposition silently
// lost its OTLP Description and Unit — nothing counted, nothing logged, and a
// missing unit changes what the downstream OTLP→Prometheus rewrite names the
// series.
func TestMetaBudgetIsChargedPerFamilyNotPerDeclaration(t *testing.T) {
	t.Parallel()
	const (
		repeats = 40_000
		help    = "a reasonably wordy help string, of the size real exporters emit for their families"
	)
	var sb strings.Builder
	sb.WriteString("# TYPE noisy_total counter\n")
	for i := range repeats {
		sb.WriteString("# HELP noisy_total " + help + "\n")
		sb.WriteString("noisy_total{i=\"" + strconv.Itoa(i) + "\"} 1\n")
	}
	// Declared LAST, after any per-line charge would have exhausted the budget.
	sb.WriteString("# HELP late_total the late family's help\n")
	sb.WriteString("# UNIT late_total seconds\n")
	sb.WriteString("# TYPE late_total counter\n")
	sb.WriteString("late_total 1\n")

	p := New(Options{OpenMetrics: true})
	var late Sample
	if _, err := p.Parse(strings.NewReader(sb.String()), func(s Sample) error {
		if s.Name == "late_total" {
			late = s
		}
		return nil
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if late.Help != "the late family's help" {
		t.Errorf("late family's Help = %q, want it intact: the repeated HELP of an earlier family spent the budget", late.Help)
	}
	if late.Unit != "seconds" {
		t.Errorf("late family's Unit = %q, want \"seconds\"", late.Unit)
	}
	// And the accounting must reflect what is RETAINED, not what was declared:
	// two families' worth, not 40k.
	if got, want := p.metaBytes, len(help)+len("noisy_total")+len("the late family's help")+len("seconds")+len("late_total"); got != want {
		t.Errorf("metaBytes = %d, want %d (one charge per retained family)", got, want)
	}
}

// A family REDECLARING different HELP is charged the growth and nothing more,
// so the budget still tracks retention when the text really does change.
func TestMetaBudgetTracksRetentionAcrossRedeclaration(t *testing.T) {
	t.Parallel()
	p := New(Options{})
	body := "# HELP f short\n# HELP f a much longer help string\nf 1\n"
	if _, err := p.Parse(strings.NewReader(body), func(Sample) error { return nil }); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got, want := p.metaBytes, len("f")+len("a much longer help string"); got != want {
		t.Errorf("metaBytes = %d, want %d (the retained text, not the sum of declarations)", got, want)
	}
}

// retainedLineBytes is the text the per-LINE reuse state holds alive after a
// parse: every string in the two positional caches and the two label buffers,
// read across their whole backing arrays — a [:0] reslice hides an entry from
// len() and not from the GC, which is the bug this measures.
func retainedLineBytes(p *Parser) (caches, labelTails int) {
	for _, c := range [][]lastKV{p.lastKV, p.exLastKV} {
		for _, kv := range c[:cap(c)] {
			caches += len(kv.name) + len(kv.value)
		}
	}
	for _, ls := range [][]Label{p.labels, p.exLabels} {
		for _, l := range ls[len(ls):cap(ls)] {
			labelTails += len(l.Name) + len(l.Value)
		}
	}
	return caches, labelTails
}

// decreasingLabelBody is the shape that pinned one long value per label
// POSITION: line k carries k labels and its LAST one is long, so the next line
// (one label fewer) never reaches that position again and neither the
// positional cache nor the label buffer's tail is ever overwritten. Every line
// is malformed (the value does not parse), so no sample, budget or counter
// other than the malformed total ever sees it. With exemplar=true the long
// labels ride on an OpenMetrics exemplar instead, whose cache is separate.
func decreasingLabelBody(positions, longLen int, exemplar bool) string {
	long := strings.Repeat("x", longLen)
	var sb strings.Builder
	for k := positions; k >= 1; k-- {
		if exemplar {
			sb.WriteString("m_total 1 # {")
		} else {
			sb.WriteString("m{")
		}
		for i := 0; i < k; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("l")
			sb.WriteString(itoa(i))
			sb.WriteString(`="`)
			if i == k-1 {
				sb.WriteString(long)
			}
			sb.WriteByte('"')
		}
		if exemplar {
			sb.WriteString("} 1\n")
		} else {
			sb.WriteString("} notanumber\n")
		}
	}
	if exemplar {
		sb.WriteString("# EOF\n")
	}
	return sb.String()
}

// The positional last-seen caches and the reused label buffers must hold a
// BOUNDED amount of text whatever the exposition does. Measured before the
// bound: 128 lines of decreasing label count and one ~1 MiB value each left
// 127 MiB retained with the parser alive — one value per position, up to
// MaxLabelsPerSample x MaxLineBytes in principle — from a scrape recording
// nothing but malformed lines. Scaled down to 64 x 256 KiB (16 MiB attempted).
func TestPositionalCachesRetainBoundedText(t *testing.T) {
	const positions, longLen = 64, 256 << 10
	for _, tc := range []struct {
		name     string
		exemplar bool
	}{{"sample labels", false}, {"exemplar labels", true}} {
		t.Run(tc.name, func(t *testing.T) {
			p := New(Options{OpenMetrics: tc.exemplar, Exemplars: tc.exemplar})
			if _, err := p.Parse(strings.NewReader(decreasingLabelBody(positions, longLen, tc.exemplar)), func(Sample) error { return nil }); err != nil {
				t.Fatal(err)
			}
			caches, tails := retainedLineBytes(p)
			// Intern-sized entries are bounded by position count x the length
			// caps; everything longer by maxLongCacheBytes.
			ceiling := maxLongCacheBytes + 2*MaxLabelsPerSample*(maxInternedNameLen+maxInternedValueLen)
			if caches > ceiling {
				t.Fatalf("positional caches retain %d bytes after the parse, want <= %d "+
					"(%d lines each leaving a %d-byte value at a position no later line reaches)",
					caches, ceiling, positions, longLen)
			}
			if tails != 0 {
				t.Fatalf("label buffers retain %d bytes PAST their length: an earlier, longer line's strings "+
					"are still referenced by the backing array", tails)
			}
		})
	}
}

// And a pooled parser, which can sit in the pool indefinitely, gives back every
// string of its last parse when it is returned (only the intern tables are
// meant to stay warm): the per-line state, the classification memo — whose name
// aliases the last line's metric name, never interned past maxInternedNameLen —
// and the per-exposition TYPE and HELP/UNIT tables. The body therefore declares
// a family and ENDS on a long un-interned name. Not parallel: another test's Get
// could otherwise take the parser out of the pool between the Put and the
// inspection.
func TestPutReleasesLineReferences(t *testing.T) {
	fam := strings.Repeat("f", maxFamilyNameBytes) // declarable, and too long to intern
	long := strings.Repeat("m", 64<<10)
	body := decreasingLabelBody(8, 4096, false) + "ok{a=\"b\"} 1\n" +
		"# TYPE " + fam + " gauge\n# HELP " + fam + " " + strings.Repeat("h", 4096) + "\n" +
		fam + "{a=\"b\"} 1\n" + long + "{a=\"b\"} 1\n"
	pp := Get(Options{})
	var last string
	if _, err := pp.Parse(strings.NewReader(body), func(s Sample) error { last = s.Name; return nil }); err != nil {
		t.Fatal(err)
	}
	p := pp.p
	// The fixture must really leave what the assertions below look for.
	if last != long || len(p.lastClass.name) != len(long) || len(p.types) != 1 || len(p.metas) != 1 {
		t.Fatalf("fixture did not take: last sample %d bytes (want %d), memo name %d bytes, %d TYPE and %d HELP entries (want 1 each)",
			len(last), len(long), len(p.lastClass.name), len(p.types), len(p.metas))
	}
	Put(pp)
	if caches, tails := retainedLineBytes(p); caches != 0 || tails != 0 || len(p.labels) != 0 {
		t.Fatalf("a returned parser still holds %d bytes in its positional caches and %d in its label buffers", caches, tails+len(p.labels))
	}
	if p.lastMetric != "" || p.longCacheBytes != 0 || p.exemplar.Labels != nil {
		t.Fatalf("a returned parser still holds line state: lastMetric=%q longCacheBytes=%d", p.lastMetric, p.longCacheBytes)
	}
	if p.lastClassOK || p.lastClass != (classified{}) {
		t.Fatalf("a returned parser still holds its classification memo: %d-byte name, %d-byte family, %d-byte help",
			len(p.lastClass.name), len(p.lastClass.family), len(p.lastClass.help))
	}
	if len(p.types) != 0 || p.typeBytes != 0 || len(p.metas) != 0 || p.metaBytes != 0 {
		t.Fatalf("a returned parser still holds the exposition's tables: %d TYPE entries (%d bytes), %d HELP/UNIT entries (%d bytes)",
			len(p.types), p.typeBytes, len(p.metas), p.metaBytes)
	}
}
