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
