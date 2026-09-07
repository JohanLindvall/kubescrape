package docscheck

import (
	"path/filepath"
	"testing"
)

// registerPattern is the whole of SourceFlags, and it silently recognised none
// of the flag.XxxVar spellings: the alternation listing the types is followed
// immediately by `(`, so `flag.StringVar` matched `String` and then wanted a
// bracket where a `V` stood. Every idiomatic package-level registration was
// therefore invisible, and the `TextVar` alternative in the pattern was dead as
// written — real TextVar takes two leading arguments and the optional group
// allowed one.
//
// The failure it produces is loud but MISLEADING, which is why it is pinned
// here spelling by spelling: docs/FLAGS.md is GENERATED from the live flag set
// and is one of the scanned docs, so the first `flag.StringVar` registration
// would have made the guard accuse a correct, freshly generated row of naming a
// flag that neither binary registers — and the obvious remedy for a red build
// is to delete the row.
func TestRegisterPatternMatchesEveryRegistrationSpelling(t *testing.T) {
	cases := []struct {
		src  string
		want string
	}{
		// Value-returning forms, on both receivers.
		{`flag.String("a-flag", "", "usage")`, "a-flag"},
		{`fs.String("a-flag", "", "usage")`, "a-flag"},
		{`flag.Bool("b-flag", false, "usage")`, "b-flag"},
		{`flag.Int("c-flag", 0, "usage")`, "c-flag"},
		{`flag.Int64("d-flag", 0, "usage")`, "d-flag"},
		{`flag.Uint("e-flag", 0, "usage")`, "e-flag"},
		{`flag.Uint64("f-flag", 0, "usage")`, "f-flag"},
		{`flag.Float64("g-flag", 0, "usage")`, "g-flag"},
		{`flag.Duration("h-flag", 30*time.Second, "usage")`, "h-flag"},

		// The *Var forms — the ones that matched nothing.
		{`flag.StringVar(&s, "i-flag", "", "usage")`, "i-flag"},
		{`fs.StringVar(&cfg.Endpoint, "j-flag", "", "usage")`, "j-flag"},
		{`flag.BoolVar(&b, "k-flag", false, "usage")`, "k-flag"},
		{`flag.IntVar(&n, "l-flag", 0, "usage")`, "l-flag"},
		{`flag.Int64Var(&n, "m-flag", 0, "usage")`, "m-flag"},
		{`flag.UintVar(&n, "n-flag", 0, "usage")`, "n-flag"},
		{`flag.Uint64Var(&n, "o-flag", 0, "usage")`, "o-flag"},
		{`flag.Float64Var(&f, "p-flag", 0, "usage")`, "p-flag"},
		{`flag.DurationVar(&d, "q-flag", time.Minute, "usage")`, "q-flag"},
		// TextVar takes TWO leading arguments, which is why the pattern allows
		// up to two.
		{`flag.TextVar(&ip, &def, "r-flag", "usage")`, "r-flag"},
		{`flag.TextVar(&lvl, slog.LevelInfo, "s-flag", "usage")`, "s-flag"},

		// The custom-value forms, which did match before and must keep doing so.
		{`flag.Var(&otlpHeaders, "otlp-header", "usage")`, "otlp-header"},
		{`fs.Var(&v, "t-flag", "usage")`, "t-flag"},
		{`flag.Func("u-flag", "usage", fn)`, "u-flag"},
		{`flag.BoolFunc("v-flag", "usage", fn)`, "v-flag"},

		// The name may sit on its own line.
		{"flag.StringVar(&s,\n\t\t\"w-flag\", \"\", \"usage\")", "w-flag"},
	}
	for _, c := range cases {
		m := registerPattern.FindStringSubmatch(c.src)
		if m == nil {
			t.Errorf("no match for %s — a flag registered that way is absent from SourceFlags, so its own generated FLAGS.md row is reported as documenting a flag nothing registers", c.src)
			continue
		}
		if m[1] != c.want {
			t.Errorf("%s → %q, want %q", c.src, m[1], c.want)
		}
	}
}

// And it must not invent flags. A name that is not the registration's own would
// put a phantom into the registered set, which is the direction that makes the
// guard PASS when it should fail.
func TestRegisterPatternDoesNotInventFlags(t *testing.T) {
	for _, src := range []string{
		`fmt.Println("not-a-flag")`,
		`cfg.String("not-a-flag")`,
		`flagsFor("not-a-flag")`,
		// A call-shaped default is out of reach by design: the second argument
		// class excludes brackets and string literals, so this matches NOTHING
		// rather than picking up "::1" as a flag name.
		`flag.TextVar(&ip, net.ParseIP("::1"), "x-flag", "usage")`,
	} {
		if m := registerPattern.FindStringSubmatch(src); m != nil {
			t.Errorf("%s matched as a registration of -%s", src, m[1])
		}
	}
}

// The pattern's reach over the real tree: the flags CmdDirs registers must be a
// superset of the ones the generated FLAGS.md documents, which is the whole
// point of scanning source instead of a binary.
func TestSourceFlagsFindsTheRegistrationsTheDocsName(t *testing.T) {
	registered, err := SourceFlags(CmdDirs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(registered) < 50 {
		t.Fatalf("SourceFlags found only %d registrations across %v; both binaries register far more, so the pattern has stopped matching a whole spelling", len(registered), CmdDirs)
	}
	documented, err := TableFlags(filepath.Join("..", "..", "docs", "FLAGS.md"))
	if err != nil {
		t.Fatal(err)
	}
	for name, where := range documented {
		if !registered[name] {
			t.Errorf("%s documents -%s, which SourceFlags does not find registered", where, name)
		}
	}
}
