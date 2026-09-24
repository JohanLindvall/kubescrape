package kubemeta

import "testing"

// The metadata routes are unauthenticated and README states they carry no
// secret material. kubectl's last-applied-configuration is a verbatim copy of
// the whole applied object, so anything inlined into a spec would be served to
// any caller that can reach the port — and stamped onto downstream telemetry.
func TestFilterAnnotationsDropsLastApplied(t *testing.T) {
	in := map[string]string{
		"kubectl.kubernetes.io/last-applied-configuration": `{"spec":{"env":[{"name":"TOKEN","value":"s3cret"}]}}`,
		"prometheus.io/scrape":                             "true",
	}
	got := FilterAnnotations(in)
	if _, ok := got["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Error("last-applied-configuration must not be served")
	}
	if got["prometheus.io/scrape"] != "true" {
		t.Errorf("ordinary annotations must survive: %v", got)
	}
	if in["kubectl.kubernetes.io/last-applied-configuration"] == "" {
		t.Error("input must not be mutated")
	}
	// An all-dropped map collapses to nil so the field stays omitempty.
	if got := FilterAnnotations(map[string]string{
		"kubectl.kubernetes.io/last-applied-configuration": "x",
	}); got != nil {
		t.Errorf("all-dropped map = %v, want nil", got)
	}
	if got := FilterAnnotations(nil); got != nil {
		t.Errorf("nil in, %v out, want nil", got)
	}
}

// kubectl is not the only tool that stamps the applied object onto the object:
// kapp (Carvel) writes the same verbatim payload under its own key on every
// object it deploys, so on a kapp-managed cluster the leak was open with the
// filter in place.
func TestFilterAnnotationsDropsEveryAppliedObjectCopy(t *testing.T) {
	for _, key := range []string{
		"kubectl.kubernetes.io/last-applied-configuration",
		"kapp.k14s.io/original",
	} {
		got := FilterAnnotations(map[string]string{
			key:                    `{"spec":{"env":[{"name":"TOKEN","value":"s3cret"}]}}`,
			"prometheus.io/scrape": "true",
		})
		if _, ok := got[key]; ok {
			t.Errorf("%s is a copy of the applied object and must not be served", key)
		}
		if got["prometheus.io/scrape"] != "true" {
			t.Errorf("%s: ordinary annotations must survive: %v", key, got)
		}
	}

	// A key that merely FINGERPRINTS the applied object carries no spec content
	// and is left alone — the denylist is for payloads, not for a vendor prefix.
	if got := FilterAnnotations(map[string]string{"kapp.k14s.io/original-diff-md5": "abc"}); got == nil {
		t.Error("a checksum annotation must survive the filter")
	}
}

// Both doors refuse the SAME keys: the read filter (FilterAnnotations, on the
// fast path and on the budgeted slow path alike) and the informer transform
// (StripDroppedAnnotations) each read refusedAnnotations. A key refused at one
// door and not the other is either refused on read while resident in every
// cached object, or stripped from the cache while a hand-built object still
// serves it — which is the drift one list exists to make impossible.
func TestEveryRefusedAnnotationIsRefusedAtBothDoors(t *testing.T) {
	blob := string(make([]byte, MaxAnnotationValueBytes+1)) // forces the slow path
	for key := range refusedAnnotations {
		m := map[string]string{key: "x", "keep": "me"}
		if !StripDroppedAnnotations(m) || len(m) != 1 || m["keep"] != "me" {
			t.Errorf("%s: the informer transform did not strip exactly the refused key: %v", key, m)
		}
		if got := FilterAnnotations(map[string]string{key: "x", "keep": "me"}); len(got) != 1 || got["keep"] != "me" {
			t.Errorf("%s: the read filter's fast path served it: %v", key, got)
		}
		got := FilterAnnotations(map[string]string{key: "x", "keep": "me", "zz/blob": blob})
		if got["keep"] != "me" || !AnnotationsOmitted(got) {
			t.Fatalf("%s: the fixture no longer reaches the budgeted slow path: %v", key, got)
		}
		if key != OmittedAnnotation && got[key] != "" {
			t.Errorf("%s: the read filter's slow path served it", key)
		}
		if got[OmittedAnnotation] == "x" {
			t.Errorf("%s: the slow path served a forged note", key)
		}
		if AnnotationWasOmitted(got, key) {
			t.Errorf("%s: a refused key was reported as omitted for SIZE", key)
		}
	}
}
