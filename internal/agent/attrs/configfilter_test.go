package attrs

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// enable/disable moved from comma-separated flags into the config section, as
// LISTS — so a pattern may itself contain a comma, which the flag form could
// never express (commas split it, and the fragments still compiled as literal
// braces, silently dropping every intended attribute).
func TestConfigFilterListsAllowCommasInPatterns(t *testing.T) {
	f, err := NewFilterFromLists(nil, []string{`k8s\.pod\.label\..{1,40}`})
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("filter is nil; the disable list should have compiled")
	}
	res := pcommon.NewResource()
	res.Attributes().PutStr("k8s.pod.label.app", "web")
	res.Attributes().PutStr("k8s.pod.name", "web-1")
	f.Apply(res)

	if _, ok := res.Attributes().Get("k8s.pod.label.app"); ok {
		t.Error("k8s.pod.label.app survived a disable pattern containing a comma")
	}
	if _, ok := res.Attributes().Get("k8s.pod.name"); !ok {
		t.Error("k8s.pod.name was dropped; only the label pattern should match")
	}
}

// The filter is shared by every pipeline, so a pipeline section cannot have its
// own. Rejecting beats silently ignoring it.
func TestPipelineSectionRejectsEnableDisable(t *testing.T) {
	for _, cfg := range []*Config{
		{Pipelines: map[string]*Config{"logs": {Enable: []string{"a"}}}},
		{Pipelines: map[string]*Config{"logs": {Disable: []string{"a"}}}},
	} {
		if _, err := NewBuilders(cfg, nil); err == nil {
			t.Error("a pipeline section setting enable/disable must be rejected")
		}
	}
	// Top level is fine.
	if _, err := NewBuilders(&Config{Disable: []string{"a"}}, nil); err != nil {
		t.Errorf("top-level enable/disable must be accepted: %v", err)
	}
}
