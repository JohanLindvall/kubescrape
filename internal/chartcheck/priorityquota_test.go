package chartcheck

import (
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// Some clusters (GKE among them) refuse a system-node-critical or
// system-cluster-critical pod outside kube-system unless a ResourceQuota in its
// namespace covers the class — and then a default install creates no agent and
// no metadata-service pod at all, with a FailedCreate event as the only symptom.
// The chart renders that quota, and this pins the RULE it must follow: the
// quota covers EXACTLY the system-* classes the rendered workloads schedule at,
// derived rather than listed beside them, so turning a class off (or a workload
// on) cannot leave the two disagreeing.
func TestPriorityClassQuotaCoversExactlyTheSystemClassesInUse(t *testing.T) {
	helm := helmBin(t)
	for _, tc := range []struct {
		name string
		set  []string
	}{
		{"default", nil},
		{"all four workloads", []string{"events.enabled=true", "events.priorityClassName=system-cluster-critical",
			"serviceGraph.enabled=true", "serviceGraph.tokenSecret.name=sg", "serviceGraph.priorityClassName=system-node-critical"}},
		{"a custom class is not a system one", []string{"agent.priorityClassName=observability-high"}},
		{"every class off", []string{"service.priorityClassName=", "agent.priorityClassName="}},
		{"quota disabled", []string{"priorityClassQuota.enabled=false"}},
	} {
		var args []string
		for _, s := range tc.set {
			args = append(args, "--set", s)
		}
		out, err := helmTemplate(helm, "observability", args...)
		if err != nil {
			t.Fatalf("%s: helm template failed: %v\n%s", tc.name, err, out)
		}

		var inUse []string
		var quotas []renderedObject
		for _, doc := range manifestcheck.Documents(string(out)) {
			var o renderedObject
			if err := yaml.Unmarshal([]byte(doc), &o); err != nil {
				t.Fatalf("%s: unmarshal: %v\n%s", tc.name, err, doc)
			}
			if o.Kind == "ResourceQuota" {
				quotas = append(quotas, o)
			}
			if c := o.Spec.Template.Spec.PriorityClassName; strings.HasPrefix(c, "system-") && !slices.Contains(inUse, c) {
				inUse = append(inUse, c)
			}
		}
		slices.Sort(inUse)

		disabled := slices.Contains(tc.set, "priorityClassQuota.enabled=false")
		if disabled || len(inUse) == 0 {
			if len(quotas) != 0 {
				t.Errorf("%s: rendered %d ResourceQuotas with the quota disabled or no system class in use (%v)", tc.name, len(quotas), inUse)
			}
			continue
		}
		if len(quotas) != 1 {
			t.Fatalf("%s: rendered %d ResourceQuotas, want 1 covering %v", tc.name, len(quotas), inUse)
		}
		q := quotas[0]
		if q.Metadata.Namespace != "observability" {
			t.Errorf("%s: quota in namespace %q, want the release namespace (a quota covers only its own namespace)", tc.name, q.Metadata.Namespace)
		}
		if len(q.Spec.ScopeSelector.MatchExpressions) != 1 {
			t.Fatalf("%s: quota has %d scope expressions, want 1", tc.name, len(q.Spec.ScopeSelector.MatchExpressions))
		}
		e := q.Spec.ScopeSelector.MatchExpressions[0]
		got := slices.Sorted(slices.Values(e.Values))
		if e.ScopeName != "PriorityClass" || e.Operator != "In" || !slices.Equal(got, inUse) {
			t.Errorf("%s: quota scope %s %s %v, want PriorityClass In %v (the classes the workloads schedule at)", tc.name, e.ScopeName, e.Operator, got, inUse)
		}
		if len(q.Spec.Hard) != 1 || q.Spec.Hard["pods"] == "" {
			t.Errorf("%s: quota hard %v, want only `pods` — anything else would require requests of pods that set none", tc.name, q.Spec.Hard)
		}
	}
}

// renderedObject is the slice of a rendered manifest these checks read.
type renderedObject struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Spec struct {
		Hard          map[string]string `json:"hard"`
		ScopeSelector struct {
			MatchExpressions []struct {
				ScopeName string   `json:"scopeName"`
				Operator  string   `json:"operator"`
				Values    []string `json:"values"`
			} `json:"matchExpressions"`
		} `json:"scopeSelector"`
		Template struct {
			Spec struct {
				PriorityClassName string `json:"priorityClassName"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}
