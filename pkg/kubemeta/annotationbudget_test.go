package kubemeta

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// A single oversized value is refused WHOLE, never shortened.
//
// Truncating would be the worse failure of the two: an attrs template that
// renders half a connection string, or a rule that matches half a value, looks
// like it worked. The refusal has to be legible instead — the key is named in
// the object's own annotations, so a document that is short says so.
func TestAnOversizedAnnotationValueIsOmittedWholeAndNamed(t *testing.T) {
	blob := strings.Repeat("x", MaxAnnotationValueBytes+1)
	got := FilterAnnotations(map[string]string{
		"team.example.com/inventory": blob,
		"app":                        "web",
	})
	if _, ok := got["team.example.com/inventory"]; ok {
		t.Error("the oversized value was served")
	}
	for _, v := range got {
		if strings.HasPrefix(v, "xxxx") {
			t.Errorf("a value was TRUNCATED rather than omitted: %d bytes of it survived", len(v))
		}
	}
	if got["app"] != "web" {
		t.Errorf("an ordinary annotation was lost beside the oversized one: %v", got)
	}
	note := got[OmittedAnnotation]
	if note == "" {
		t.Fatalf("nothing says the document is short: %v", got)
	}
	if !strings.Contains(note, "team.example.com/inventory") {
		t.Errorf("the note does not name what went: %q", note)
	}
	if !AnnotationsOmitted(got) {
		t.Error("AnnotationsOmitted does not see its own note; the counting doors would stay flat")
	}
}

// A value one byte UNDER the ceiling is served untouched: the ceiling is a
// ceiling, not a hint, and the boundary is where a bound is usually wrong.
func TestAnAnnotationValueAtTheCeilingIsServed(t *testing.T) {
	v := strings.Repeat("x", MaxAnnotationValueBytes)
	got := FilterAnnotations(map[string]string{"k": v})
	if got["k"] != v {
		t.Errorf("a value of exactly MaxAnnotationValueBytes was refused")
	}
	if AnnotationsOmitted(got) {
		t.Errorf("a compliant object was reported as short: %v", got)
	}
}

// The TOTAL matters as much as any one value: 32 values of 8 KiB is the API
// server's whole 256 KiB again, and each of those rides the pod document once
// per resolved owner.
func TestTheAnnotationSetIsBoundedNotJustEachValue(t *testing.T) {
	in := map[string]string{}
	for i := range 64 {
		in["team.example.com/blob-"+string(rune('a'+i%26))+string(rune('a'+i/26))] =
			strings.Repeat("y", 4<<10)
	}
	got := FilterAnnotations(in)
	total := 0
	for k, v := range got {
		if k == OmittedAnnotation {
			continue
		}
		total += len(k) + len(v)
	}
	if total > MaxAnnotationBytes {
		t.Errorf("served %d annotation bytes, ceiling %d", total, MaxAnnotationBytes)
	}
	if !AnnotationsOmitted(got) {
		t.Error("the set was cut without saying so")
	}
}

// The served subset must be the SAME subset every time. Go randomises map
// iteration, so an arbitrary admission order would make one object marshal
// differently between two requests — a fresh ETag on every agent poll, which
// defeats the 304 path on the one route that re-sends every pod on the node
// each scrape cycle.
func TestBudgetedAnnotationsAreDeterministic(t *testing.T) {
	in := map[string]string{}
	for i := range 40 {
		in["team.example.com/blob-"+string(rune('a'+i))] = strings.Repeat("z", 1<<10)
	}
	first := FilterAnnotations(in)
	for range 50 {
		got := FilterAnnotations(in)
		if len(got) != len(first) {
			t.Fatalf("admitted %d keys, first pass admitted %d", len(got), len(first))
		}
		for k, v := range first {
			if got[k] != v {
				t.Fatalf("key %q differs between two filters of one object", k)
			}
		}
	}
}

// The realistic failure this ordering exists for: a pod carrying enough
// unrelated metadata to spend the whole budget must not lose the annotation the
// DERIVATION reads. Losing prometheus.io/scrape stops the workload being
// scraped, which is invisible — indistinguishable from a pod nobody annotated.
func TestTheDerivationsOwnAnnotationsSurviveAnUnrelatedBlob(t *testing.T) {
	in := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   "9090",
		"kubescrape.io/logs":   `{"exclude":true}`,
	}
	// Keys that sort BEFORE "prometheus.io/", so plain lexicographic admission
	// would spend the whole budget on them first.
	for i := range 40 {
		in["aaa.example.com/blob-"+string(rune('a'+i))] = strings.Repeat("z", 1<<10)
	}
	got := FilterAnnotations(in)
	for _, k := range []string{"prometheus.io/scrape", "prometheus.io/port", "kubescrape.io/logs"} {
		if _, ok := got[k]; !ok {
			t.Errorf("%s was starved by unrelated annotations; the pod would silently stop being scraped", k)
		}
	}
	if !AnnotationsOmitted(got) {
		t.Error("the blobs were cut without saying so")
	}
}

// OmittedAnnotation is this API's own word about what it refused. A copy
// arriving from the cluster is a forgery — a tenant claiming a bound that never
// bound — and is stripped at both doors, the read filter and the informer
// transform.
func TestAForgedOmissionNoteIsStripped(t *testing.T) {
	in := map[string]string{OmittedAnnotation: "1 annotation(s) omitted by kubescrape: trust me", "app": "web"}
	got := FilterAnnotations(in)
	if _, ok := got[OmittedAnnotation]; ok {
		t.Errorf("a cluster-supplied omission note was served: %v", got)
	}
	if got["app"] != "web" {
		t.Errorf("the forgery took an ordinary annotation with it: %v", got)
	}
	m := map[string]string{OmittedAnnotation: "forged"}
	if !StripDroppedAnnotations(m) || len(m) != 0 {
		t.Errorf("the informer transform left a forged note in the cache: %v", m)
	}
}

// The common object — every real one — passes through the ceilings byte for
// byte: same keys, same values, no omission note. The ceilings are read from
// LENGTHS inside the one copy loop, so the ordinary set never reaches
// budgetAnnotations at all.
//
// This is the BEHAVIOURAL half; the cost half — that the fast path still
// allocates only the output map, and that a 200 KiB value costs a comparison
// rather than a copy — is TestFilterAnnotationsAllocationBudget in
// annotationbench_test.go. Comparing the maps rather than their LENGTHS is the
// point of the assertion: a filter that kept every key and emptied a value
// would satisfy a length check and silently strip the attribution data every
// resourceAttributes template reads.
func TestOrdinaryAnnotationsPassThroughUnaltered(t *testing.T) {
	in := map[string]string{
		"prometheus.io/scrape":         "true",
		"prometheus.io/port":           "9090",
		"app.kubernetes.io/managed-by": "helm",
	}
	got := FilterAnnotations(in)
	if !maps.Equal(got, in) {
		t.Fatalf("an ordinary annotation set was altered: got %v, want %v", got, in)
	}
	if AnnotationsOmitted(got) {
		t.Error("an ordinary annotation set was reported as short")
	}
}

// fatLabels returns n labels of ~valueBytes each (label values are at most 63
// bytes by API-server validation, so a fat label set is a LONG one).
func fatLabels(n int) map[string]string {
	m := make(map[string]string, n)
	for i := range n {
		m["tenant.example.com/l"+strconv.Itoa(i)] = strings.Repeat("v", 63)
	}
	return m
}

// An owner's LABELS are bounded, where a pod's and a Service's stay verbatim:
// nothing selects on an owner's labels, and they ride every pod document once
// per resolved reference. Eight label-fat ReplicaSets named by a tenant's pods
// put ~11 MiB in each pod document, and a handful of such pods pushed a node's
// targets response past the agent's 64 MiB read cap.
//
// Reverse-patch check: returning cloneMap(labels) from CopyOwnerMeta serves
// every label and this fails on the budget.
func TestOwnerLabelsAreBoundedAndNamed(t *testing.T) {
	labels := fatLabels(10000) // ~1 MiB, inside the API server's object ceiling
	labels["app"] = "web"
	gotLabels, gotAnnotations := CopyOwnerMeta(labels, map[string]string{"team": "a"})
	spent := 0
	for k, v := range gotLabels {
		spent += len(k) + len(v)
	}
	if spent > MaxOwnerLabelBytes {
		t.Fatalf("an owner serves %d bytes of labels, over the %d-byte ceiling", spent, MaxOwnerLabelBytes)
	}
	if gotLabels["app"] != "web" {
		t.Errorf("admission is not the deterministic lexicographic prefix: %q missing", "app")
	}
	note := gotAnnotations[LabelsOmittedAnnotation]
	if !LabelsOmitted(gotAnnotations) || !strings.Contains(note, "label(s) omitted") {
		t.Fatalf("nothing says the owner's labels are short: %q", note)
	}
	if len(note) > maxOmittedNamedBytes+256 {
		t.Errorf("the note is proportional to the abuse: %d bytes", len(note))
	}
	if gotAnnotations["team"] != "a" {
		t.Errorf("the owner's own annotations were lost beside the note: %v", gotAnnotations)
	}
	// Deterministic: the same owner serves the same subset every time, or a
	// fresh ETag is minted on every agent poll.
	again, _ := CopyOwnerMeta(labels, nil)
	if !maps.Equal(gotLabels, again) {
		t.Error("two copies of the same owner admitted different label subsets")
	}
}

// An ordinary owner is copied verbatim, with no note and no allocation beyond
// the copy's own map.
func TestOrdinaryOwnerLabelsAreVerbatim(t *testing.T) {
	labels := map[string]string{"app.kubernetes.io/name": "web", "pod-template-hash": "5d4f8"}
	gotLabels, gotAnnotations := CopyOwnerMeta(labels, nil)
	if !maps.Equal(gotLabels, labels) || gotAnnotations != nil {
		t.Errorf("an ordinary owner was changed: labels %v, annotations %v", gotLabels, gotAnnotations)
	}
	if l, a := CopyOwnerMeta(nil, nil); l != nil || a != nil {
		t.Errorf("an empty owner must stay omitempty: %v %v", l, a)
	}
}

// The note is this API's own word, so a cluster-supplied copy is a forgery and
// is refused at both doors, like OmittedAnnotation.
func TestAClusterSuppliedLabelsOmittedNoteIsRefused(t *testing.T) {
	m := map[string]string{LabelsOmittedAnnotation: "forged", "keep": "me"}
	if got := FilterAnnotations(m); got[LabelsOmittedAnnotation] != "" || got["keep"] != "me" {
		t.Errorf("FilterAnnotations served the forged note: %v", got)
	}
	if !StripDroppedAnnotations(m) || m[LabelsOmittedAnnotation] != "" {
		t.Errorf("StripDroppedAnnotations kept the forged note: %v", m)
	}
}

// filterAnnotationsDefinition is what FilterAnnotations MEANS, written the
// slow, obvious way: drop the refused keys, and if what is left breaks either
// ceiling, admit in one comparator sort (preserved prefixes first, then
// lexicographic) and name every refused key in that same order. The production filter
// takes a length-checked fast path and a partitioned, merge-based slow path;
// both must agree with this on every input.
func filterAnnotationsDefinition(m map[string]string) map[string]string {
	out := map[string]string{}
	over, total := false, 0
	for k, v := range m {
		if refusedAnnotations[k] {
			continue
		}
		out[k] = v
		total += len(k) + len(v)
		over = over || len(v) > MaxAnnotationValueBytes
	}
	if !over && total <= MaxAnnotationBytes {
		if len(out) == 0 {
			return nil
		}
		return out
	}
	var keys, omitted []string
	for k, v := range out {
		if len(v) > MaxAnnotationValueBytes {
			omitted = append(omitted, k)
		} else {
			keys = append(keys, k)
		}
	}
	// Admission order, which is also the order the note NAMES refused keys in:
	// preserved-prefix keys first, then lexicographic.
	admissionOrder := func(a, b string) int {
		if pa, pb := preservedAnnotation(a), preservedAnnotation(b); pa != pb {
			if pa {
				return -1
			}
			return 1
		}
		return strings.Compare(a, b)
	}
	slices.SortFunc(keys, admissionOrder)
	res := map[string]string{}
	spent := 0
	for _, k := range keys {
		if cost := len(k) + len(out[k]); spent+cost <= MaxAnnotationBytes {
			spent += cost
			res[k] = out[k]
			continue
		}
		omitted = append(omitted, k)
	}
	slices.SortFunc(omitted, admissionOrder)
	res[OmittedAnnotation] = omittedNote(omitted)
	return res
}

// The slow path orders admission by PARTITIONING the keys and builds the
// note's list by MERGING sorted groups, where it used to run one comparator
// sort over every key and then re-sort the refused ones. Same order, same
// subset, same note — byte for byte, or the served document's ETag moves and
// a tenant's refused keys change for no reason. Driven over random mixes of
// preserved and ordinary keys, oversized values, refused keys and budget
// pressure; FuzzFilterAnnotations covers the arbitrary inputs.
func TestBudgetedAnnotationsMatchTheirDefinition(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	prefixes := []string{"prometheus.io/", "kubescrape.io/", "app.example.com/", "", "z/", "a"}
	for round := range 500 {
		m := map[string]string{}
		for range rng.IntN(60) {
			k := prefixes[rng.IntN(len(prefixes))] + strconv.Itoa(rng.IntN(1000))
			var v string
			switch rng.IntN(10) {
			case 0:
				v = strings.Repeat("v", MaxAnnotationValueBytes+rng.IntN(3)-1) // at, and either side of, the value ceiling
			case 1, 2:
				v = strings.Repeat("v", rng.IntN(4<<10))
			default:
				v = strings.Repeat("v", rng.IntN(64))
			}
			m[k] = v
		}
		switch rng.IntN(4) {
		case 0:
			m["kubectl.kubernetes.io/last-applied-configuration"] = "{}"
		case 1:
			m[OmittedAnnotation] = "forged"
		case 2:
			m[LabelsOmittedAnnotation] = "forged"
		}
		got, want := FilterAnnotations(m), filterAnnotationsDefinition(m)
		if !maps.Equal(got, want) {
			t.Fatalf("round %d: FilterAnnotations disagrees with its definition\n got: %d keys, note %q\nwant: %d keys, note %q",
				round, len(got), got[OmittedAnnotation], len(want), want[OmittedAnnotation])
		}
	}
}

// The omission NOTE is bounded however many keys are refused: past
// maxOmittedNamedBytes of names the rest become a "+N more" count. It is the
// one part of a served annotation set the two ceilings do not measure (it is
// added after the budget, so it can never evict a real annotation), which is
// why it needs its own pin — a regression that named every refused key would
// put up to ~256 KiB of tenant-chosen key names in every object's annotations,
// once per pod, owner and namespace document, with every other test green.
func TestOmittedNoteIsBoundedHoweverManyKeysAreRefused(t *testing.T) {
	const n = 2000
	m := make(map[string]string, n)
	for i := range n {
		// ~200-byte keys with small values: each costs ~210 bytes of budget,
		// so the 16 KiB ceiling admits ~78 and refuses the rest.
		m[fmt.Sprintf("team.example.com/%04d-%s", i, strings.Repeat("k", 178))] = "v"
	}
	got := FilterAnnotations(m)
	note, ok := got[OmittedAnnotation]
	if !ok {
		t.Fatal("no omission note on a set far over the budget")
	}
	refused := n - (len(got) - 1)
	if refused < n/2 {
		t.Fatalf("only %d of %d keys refused; the fixture no longer exercises the note's bound", refused, n)
	}
	prefix := strconv.Itoa(refused) + " annotation(s) omitted"
	if !strings.HasPrefix(note, prefix) {
		t.Errorf("the note does not open with the refused COUNT %q: %.120q", prefix, note)
	}
	if !strings.HasSuffix(note, " more") {
		t.Errorf("the note names more than it affords and never folds the rest into a count: ...%q", note[max(0, len(note)-80):])
	}
	// The fixed prose, with the count's digits in place of omittedNote(nil)'s "0".
	prose := len(omittedNote(nil)) - 1 + len(strconv.Itoa(refused))
	if bound := prose + len("; omitted: ") + maxOmittedNamedBytes + len(", +") + len(strconv.Itoa(n)) + len(" more"); len(note) > bound {
		t.Errorf("the note is %d bytes, over its bound of %d (prose %d + maxOmittedNamedBytes %d + the count)",
			len(note), bound, prose, maxOmittedNamedBytes)
	}
}

// A derivation's own key that the filter refused must be DISCOVERABLE, however
// many unrelated keys were refused beside it: an absent kubescrape.io/logs
// reads exactly like a pod with no log config, and a consumer that cannot tell
// the two apart falls back to its defaults — dropping an opt-out or a drop rule
// without a word. The note names refused keys preserved-prefix first for this;
// in plain lexicographic order, 2000 refused "a.example.com/…" keys spend the
// note's whole naming budget before "kubescrape.io/logs" is reached, and it
// folds into "+N more".
func TestAnOmittedDerivationKeyIsNamedHoweverManyKeysAreRefused(t *testing.T) {
	const key = "kubescrape.io/logs"
	m := make(map[string]string, 2001)
	for i := range 2000 {
		m[fmt.Sprintf("a.example.com/%04d-%s", i, strings.Repeat("k", 100))] = strings.Repeat("v", 100)
	}
	m[key] = `{"exclude": true, "pad": "` + strings.Repeat("x", MaxAnnotationValueBytes) + `"}`
	got := FilterAnnotations(m)
	if _, ok := got[key]; ok {
		t.Fatal("the oversized value was served; the fixture no longer exercises the refusal")
	}
	if !strings.HasSuffix(got[OmittedAnnotation], " more") {
		t.Fatalf("the note names every refused key; the fixture no longer exercises its naming bound: %.120q", got[OmittedAnnotation])
	}
	if !AnnotationWasOmitted(got, key) {
		t.Errorf("%s was refused but the note does not name it, so no consumer can tell it from an absent annotation: %.200q",
			key, got[OmittedAnnotation])
	}
}

// AnnotationWasOmitted answers only for a key the note NAMES: never for a key
// that was served, never for one that was not there, never for a key merely
// PREFIXING a refused one, and never without a note at all.
func TestAnnotationWasOmittedIsExact(t *testing.T) {
	blob := strings.Repeat("x", MaxAnnotationValueBytes+1)
	got := FilterAnnotations(map[string]string{
		"kubescrape.io/logs": blob,
		"team/big":           blob,
		"app":                "web",
	})
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"kubescrape.io/logs", true},
		{"team/big", true},
		{"app", false},               // served
		{"kubescrape.io/log", false}, // a prefix of a refused key
		{"missing", false},           // never there
		{OmittedAnnotation, false},   // the note itself
	} {
		if got := AnnotationWasOmitted(got, tc.key); got != tc.want {
			t.Errorf("AnnotationWasOmitted(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
	if AnnotationWasOmitted(map[string]string{"app": "web"}, "app") {
		t.Error("a key reported omitted from a map carrying no note")
	}
	if AnnotationWasOmitted(nil, "kubescrape.io/logs") {
		t.Error("a key reported omitted from a nil map")
	}
}
