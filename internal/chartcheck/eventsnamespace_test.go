package chartcheck

import (
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// events.namespace narrows the reader to ONE namespace (-events-namespace), and
// every events call the reader makes is CoreV1().Events(<that namespace>). The
// chart still rendered the cluster-wide events ClusterRole + binding then, so
// the singleton's ServiceAccount could read every namespace's events while
// asking for one. With a namespace set the read grant must be a Role in THAT
// namespace, bound to the release namespace's ServiceAccount, and no ClusterRole
// may grant events at all.
func TestEventsNamespaceNarrowsTheReadGrantToThatNamespace(t *testing.T) {
	helm := helmBin(t)
	const release, releaseNS, watched = releaseName, "monitoring", "payments"
	out, err := helmTemplate(helm, releaseNS,
		"--set", "events.enabled=true",
		"--set", "events.namespace="+watched,
	)
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	manifest := string(out)
	if !strings.Contains(manifest, "- -events-namespace="+watched+"\n") {
		t.Fatalf("the reader is not rendered with -events-namespace=%s; this test would judge the wrong shape", watched)
	}

	type objectRef struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	}
	type rbacObject struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
		Rules    []rbacRule  `json:"rules"`
		RoleRef  objectRef   `json:"roleRef"`
		Subjects []objectRef `json:"subjects"`
	}
	var readRoles []rbacObject
	bindings := map[string]rbacObject{} // "<ns>/<roleRef.name>" -> RoleBinding
	for _, doc := range manifestcheck.Documents(manifest) {
		var o rbacObject
		if err := yaml.Unmarshal([]byte(doc), &o); err != nil {
			continue
		}
		grantsEvents := false
		for _, r := range o.Rules {
			if slices.Contains(r.Resources, "events") {
				grantsEvents = true
			}
		}
		switch o.Kind {
		case "ClusterRole":
			if grantsEvents {
				t.Errorf("ClusterRole %s grants events although the reader watches only %q — every other namespace's events are readable by the singleton",
					o.Metadata.Name, watched)
			}
		case "Role":
			if grantsEvents {
				readRoles = append(readRoles, o)
			}
		case "RoleBinding":
			bindings[o.Metadata.Namespace+"/"+o.RoleRef.Name] = o
		}
	}

	if len(readRoles) != 1 {
		t.Fatalf("found %d Roles granting events, want exactly 1 in %q", len(readRoles), watched)
	}
	role := readRoles[0]
	if role.Metadata.Namespace != watched {
		t.Errorf("the events Role is in namespace %q, want %q — a Role grants only within its own namespace, so anywhere else the reader is refused",
			role.Metadata.Namespace, watched)
	}
	for _, r := range role.Rules {
		if slices.Contains(r.Resources, "events") && (!equalSets(r.Verbs, []string{"list", "watch"}) || len(r.APIGroups) != 1 || r.APIGroups[0] != "") {
			t.Errorf("events Role rule %+v, want core/v1 events list+watch only (the reader's two calls)", r)
		}
	}
	b, ok := bindings[role.Metadata.Namespace+"/"+role.Metadata.Name]
	if !ok {
		t.Fatalf("no RoleBinding in %q references Role %s — the grant binds nobody", role.Metadata.Namespace, role.Metadata.Name)
	}
	wantSubject := objectRef{Kind: "ServiceAccount", Name: release + "-events", Namespace: releaseNS}
	if len(b.Subjects) != 1 || b.Subjects[0] != wantSubject {
		t.Errorf("RoleBinding %s subjects %+v, want exactly %+v (the singleton's ServiceAccount lives in the RELEASE namespace)",
			b.Metadata.Name, b.Subjects, wantSubject)
	}

	// The coordination objects are unaffected: they live in the release
	// namespace, name-scoped, whatever the reader watches.
	assertEventsRBACIsScoped(t, "rendered chart (events.namespace set)", manifest)
}
