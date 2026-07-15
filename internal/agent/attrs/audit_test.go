package attrs

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// TestAuditFilterOverlapDisableWins: when a key matches both the enable and the
// disable set, disable wins (it is removed).
func TestAuditFilterOverlapDisableWins(t *testing.T) {
	f, err := NewFilter(`k8s\..*`, `k8s\.pod\.label\..*`)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Keep("k8s.pod.name") {
		t.Error("k8s.pod.name should be kept (enable matches, disable does not)")
	}
	if f.Keep("k8s.pod.label.team") {
		t.Error("k8s.pod.label.team matched both sets; disable must win")
	}
	if f.Keep("service.name") {
		t.Error("service.name is outside the enable set; must be dropped")
	}
}

// TestAuditFilterCanStripIdentity documents a footgun: because Filter.Apply runs
// AFTER Identity/PrefixInstance in Build, a restrictive enable set that omits the
// service.* keys strips the derived Mimir job/instance identity. Not a bug (the
// filter is applied to all keys by contract) but worth knowing.
func TestAuditFilterCanStripIdentity(t *testing.T) {
	f, err := NewFilter(`k8s\..*`, "") // only k8s.* survive
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBuilder(nil, f)
	if err != nil {
		t.Fatal(err)
	}
	res := pcommon.NewResource()
	b.Build(res, testCtx())
	got := res.Attributes().AsRaw()
	if _, ok := got["service.instance.id"]; ok {
		t.Fatalf("service.instance.id unexpectedly survived enable=k8s.*: %v", got)
	}
	if _, ok := got["service.namespace"]; ok {
		t.Fatalf("service.namespace unexpectedly survived: %v", got)
	}
	t.Log("NOTE: an enable filter excluding service.* strips derived job/instance identity")
}

// TestAuditStaticEmptyValueEmitted: a static attribute with an empty value is
// emitted verbatim as "" (unlike a template evaluating empty, which is omitted).
func TestAuditStaticEmptyValueEmitted(t *testing.T) {
	got := build(t, &Config{
		Static:     map[string]string{"empty.static": ""},
		Attributes: map[string]string{"empty.tmpl": `{{ index .Pod.Labels "nope" }}`},
	}, nil, testCtx())
	if v, ok := got["empty.static"]; !ok || v.(string) != "" {
		t.Errorf("empty static attribute should be emitted as \"\": %v", got["empty.static"])
	}
	if _, ok := got["empty.tmpl"]; ok {
		t.Errorf("empty template attribute should be omitted: %v", got["empty.tmpl"])
	}
}

// TestAuditServiceNameBareReplicaSet: a pod owned ONLY by a ReplicaSet (no
// Deployment resolved) falls back to the pod NAME, not the ReplicaSet name —
// the documented behavior ("A ReplicaSet owner is not used").
func TestAuditServiceNameBareReplicaSet(t *testing.T) {
	pod := kubemeta.Pod{
		Name:   "web-abc123-xyz",
		Owners: []kubemeta.Owner{{Kind: "ReplicaSet", Name: "web-abc123"}},
	}
	if got := ServiceName(pod); got != "web-abc123-xyz" {
		t.Errorf("ServiceName = %q, want the pod name (bare ReplicaSet is skipped)", got)
	}
	// With the Deployment resolved, that wins.
	pod.Owners = append(pod.Owners, kubemeta.Owner{Kind: "Deployment", Name: "web"})
	if got := ServiceName(pod); got != "web" {
		t.Errorf("ServiceName = %q, want the Deployment name", got)
	}
}

// TestAuditTemplateNilContextNoPanic: templates dereferencing a Context field
// that is nil for this resource (no Pod/Container/Service/Node) must render as
// omitted, never panic.
func TestAuditTemplateNilContextNoPanic(t *testing.T) {
	got := build(t, &Config{
		Attributes: map[string]string{
			"a": `{{ .Pod.Name }}`,
			"b": `{{ .Node.Name }}`,
			"c": `{{ .Container.ID }}`,
			"d": `{{ .Service.Name }}`,
			"e": `{{ index .Pod.Labels "x" }}`,
		},
	}, nil, Context{}) // everything nil
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if _, ok := got[k]; ok {
			t.Errorf("attr %q should be omitted on an empty Context: %v", k, got[k])
		}
	}
}

// TestAuditCadvisorDefaultSurvivesTopLevelBase re-confirms the precedence rule
// the audit targeted: a top-level base instancePrefix must NOT strip cadvisor's
// built-in collision protection.
func TestAuditCadvisorDefaultSurvivesTopLevelBase(t *testing.T) {
	base := "globalbase"
	bs, err := NewBuilders(&Config{InstancePrefix: &base}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := instanceOf(t, bs.Cadvisor); got != "cadvisor-cid" {
		t.Errorf("cadvisor instance = %q, want cadvisor-cid (default beats base)", got)
	}
	if got := instanceOf(t, bs.Logs); got != "globalbase-cid" {
		t.Errorf("logs instance = %q, want globalbase-cid (base applies where no default)", got)
	}
}
