package transform

import (
	"math/big"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.starlark.net/starlark"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// A FAILED assignment must leave NO partial mutation. SetKey adds the key with
// PutEmpty before converting the value, so a conversion error used to leave an
// Empty-valued attribute behind — which the ingest `admit` hook, fail-open and
// (then) writable, forwarded while reporting that the hook did nothing.
// Regression for SetKey's convertibility pre-check. Driven through SetKey
// directly: admit's view is read-only now (TestAdmitResourceIsReadOnly), so an
// assignment there is refused before SetKey runs and would pass vacuously.
func TestFailedAssignmentLeavesNoPartial(t *testing.T) {
	m := pcommon.NewMap()
	if err := (attrsView{m}).SetKey(starlark.String("team"), starlark.NewDict(0)); err == nil {
		t.Fatal("assigning a dict must fail")
	}
	if _, ok := m.Get("team"); ok {
		t.Errorf("a failed assignment left a partial attribute on the map: %v", m.AsRaw())
	}
}

// The sharper half of the same contract: a failed assignment to an EXISTING key
// must leave that key's value ALONE. The first fix returned the error and left
// an Empty value; the second removed the key, which DELETED a good pre-existing
// value — data loss on any payload a failed script leaves behind.
func TestFailedAssignmentPreservesAnExistingValue(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("service.name", "checkout")
	m.PutStr("keep", "me")

	if err := (attrsView{m}).SetKey(starlark.String("service.name"), starlark.NewDict(0)); err == nil {
		t.Fatal("assigning a dict must fail")
	}
	v, ok := m.Get("service.name")
	if !ok {
		t.Fatalf("a failed assignment DELETED a pre-existing attribute: %v", m.AsRaw())
	}
	if v.Str() != "checkout" {
		t.Errorf("service.name = %q after a failed assignment, want the untouched %q", v.Str(), "checkout")
	}
	if got, _ := m.Get("keep"); got.Str() != "me" {
		t.Errorf("an unrelated attribute was disturbed: %v", m.AsRaw())
	}
}

// An out-of-range int is the non-type-mismatch failure, and the pre-check has
// to model it too or it silently diverges from fromStarlark.
func TestOutOfRangeIntLeavesTheExistingValue(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("n", "original")
	huge := starlark.MakeBigInt(new(big.Int).Lsh(big.NewInt(1), 200))
	if err := (attrsView{m}).SetKey(starlark.String("n"), huge); err == nil {
		t.Fatal("assigning 1<<200 must fail")
	}
	if got, ok := m.Get("n"); !ok || got.Str() != "original" {
		t.Errorf("an out-of-range int assignment disturbed the existing value: %v", m.AsRaw())
	}
}

// admit is a PREDICATE, and the resource it judges is read-only. It used to be
// handed the writable view, so a script that wrote and then erred forwarded
// its write — breaking the fail-open contract ("a hook error did nothing")
// that the targets hook keeps by restoring the pre-script target — and a
// successful write persisted too, undocumented, after the receipt-time strip
// the hook runs behind. An assignment is now a script error: counted, admitted
// (fail-open), and the resource is exactly what arrived.
func TestAdmitResourceIsReadOnly(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"a write then an error", "      resource[\"team\"] = \"written\"\n      fail(\"boom\")\n"},
		{"a write then True", "      resource[\"tier\"] = \"gold\"\n      return True\n"},
		{"a delete then True", "      resource[\"keep\"] = None\n      return True\n"},
		{"a write then False", "      resource[\"team\"] = \"written\"\n      return False\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := hookWrapper(t, "ingest: |\n  def admit(resource):\n"+tc.body)
			m := pcommon.NewMap()
			m.PutStr("keep", "me")
			before := obs.TransformErrors.WithLabelValues("ingest").Value()
			if !w.AdmitResource(m) {
				t.Error("an assignment is a script error, and a hook script error must fail OPEN (admit)")
			}
			if got := obs.TransformErrors.WithLabelValues("ingest").Value() - before; got != 1 {
				t.Errorf("kubescrape_transform_errors_total{signal=ingest} moved by %v, want 1", got)
			}
			if got := m.AsRaw(); len(got) != 1 || got["keep"] != "me" {
				t.Errorf("admit changed the resource it only judges: %v", got)
			}
		})
	}
	// Reading is unaffected.
	w := hookWrapper(t, "ingest: |\n  def admit(resource):\n      return \"keep\" in resource and resource[\"keep\"] != \"me\"\n")
	m := pcommon.NewMap()
	m.PutStr("keep", "me")
	if w.AdmitResource(m) {
		t.Error("a read-only predicate returning False must reject")
	}
}

// dupKeyLogs decodes a one-record logs payload whose record and resource
// attributes are ORDERED lists, so a key may repeat. OTLP/JSON is the one
// encoder reachable from here that can express that — every pcommon.Map setter
// resolves through Get and overwrites — and it models attributes as the
// repeated field they are on the wire, which pdata's decoder keeps verbatim.
func dupKeyLogs(t *testing.T, res, rec string) plog.Logs {
	t.Helper()
	doc := `{"resourceLogs":[{"resource":{"attributes":[` + res +
		`]},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hi"},"attributes":[` + rec + `]}]}]}]}`
	var um plog.JSONUnmarshaler
	ld, err := um.UnmarshalLogs([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return ld
}

func strKV(k, v string) string {
	return `{"key":"` + k + `","value":{"stringValue":"` + v + `"}}`
}

// keysOf lists m's keys in storage order — Get cannot see a duplicate.
func keysOf(m pcommon.Map) []string {
	var ks []string
	for k := range m.All() {
		ks = append(ks, k)
	}
	return ks
}

// A script's delete and overwrite act on EVERY occurrence of a key. pdata's
// Remove and PutEmpty stop at the FIRST match, and an OTLP attribute list is a
// repeated field, so a sender that wrote an attribute twice kept a copy through
// `attrs[k] = None` (the script itself still read `k in attrs` as True) and
// shipped its original value beside an overwrite — an operator's redaction
// bypassed by repetition, the hole otlpingest's removeAll closed for the
// receipt-time strip.
func TestScriptDeleteAndOverwriteReachEveryDuplicateKey(t *testing.T) {
	prog, err := Compile([]byte(`logs: |
  def transform(batch):
      for r in batch:
          r.attributes["user.email"] = None
          r.attributes["card"] = "[REDACTED]"
          r.attributes["still_there"] = str("user.email" in r.attributes)
          r.resource["tenant"] = None
`))
	if err != nil {
		t.Fatal(err)
	}
	ld := dupKeyLogs(t,
		strKV("tenant", "a")+","+strKV("keep", "me")+","+strKV("tenant", "b"),
		strKV("user.email", "a@b.c")+","+strKV("card", "4111")+","+strKV("level", "info")+","+
			strKV("user.email", "x@y.z")+","+strKV("card", "4222"))
	if n := countKeyIn(ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes(), "card"); n != 2 {
		t.Fatalf("test premise broken: the decoder kept %d copies of card, want 2", n)
	}
	if _, err := prog.logs.runLogs(ld, nil); err != nil {
		t.Fatal(err)
	}
	attrs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	if n := countKeyIn(attrs, "user.email"); n != 0 {
		t.Errorf("%d copies of user.email survived `= None`: %v", n, keysOf(attrs))
	}
	if n := countKeyIn(attrs, "card"); n != 1 {
		t.Errorf("%d copies of card after an overwrite, want exactly 1: %v", n, keysOf(attrs))
	}
	if v, _ := attrs.Get("card"); v.Str() != "[REDACTED]" {
		t.Errorf("card = %q, want the overwrite", v.Str())
	}
	if v, _ := attrs.Get("still_there"); v.Str() != "False" {
		t.Errorf(`the script read "user.email" in attributes as %s after deleting it`, v.Str())
	}
	res := ld.ResourceLogs().At(0).Resource().Attributes()
	if n := countKeyIn(res, "tenant"); n != 0 {
		t.Errorf("%d copies of tenant survived on the resource: %v", n, keysOf(res))
	}
	if v, ok := res.Get("keep"); !ok || v.Str() != "me" {
		t.Errorf("an unrelated resource attribute was disturbed: %v", keysOf(res))
	}
}

// An overwrite of an ORDINARY key keeps its position, and a delete keeps the
// order of what is left: the duplicate sweep must not turn every assignment
// into a move to the end of the map.
func TestAttributeWritesPreserveOrder(t *testing.T) {
	m := pcommon.NewMap()
	for _, k := range []string{"a", "b", "c", "d"} {
		m.PutStr(k, k)
	}
	v := attrsView{m}
	if err := v.SetKey(starlark.String("b"), starlark.String("B")); err != nil {
		t.Fatal(err)
	}
	if err := v.SetKey(starlark.String("a"), starlark.None); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(keysOf(m), ","); got != "b,c,d" {
		t.Fatalf("keys after overwrite(b) and delete(a) = %s, want b,c,d", got)
	}
	if got, _ := m.Get("b"); got.Str() != "B" {
		t.Fatalf("b = %q", got.Str())
	}
}

func countKeyIn(m pcommon.Map, k string) int {
	n := 0
	for key := range m.All() {
		if key == k {
			n++
		}
	}
	return n
}
