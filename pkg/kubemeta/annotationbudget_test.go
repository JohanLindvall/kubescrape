package kubemeta

import (
	"maps"
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
