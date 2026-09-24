package chartcheck

import (
	"maps"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// The agent's OTLP ingest listeners are reachable at its pod IP and nothing
// else unless the chart gives an application an address, and the two ways it
// offers are NOT interchangeable (values.yaml, agent.ingest; CONFIGURATION.md,
// "How an application addresses the local agent"):
//
//   - agent.ingest.service.enabled renders a Service whose ONE property that
//     matters is internalTrafficPolicy: Local. Without it the Service is the
//     plain round-robin ClusterIP the docs warn against — a push lands on some
//     other node's agent — and nothing about the rendered object would look
//     wrong.
//   - agent.ingest.hostPort binds both listeners on the node.
//
// Both branches used to be rendered by no fixture and no test, so these pin the
// RULES the goldens only freeze an instance of.

// ingestAgentRender renders every workload (so the Service's selector is
// checked against the other agent-binary workloads too) with ingest on.
func ingestAgentRender(t *testing.T, helm string, set ...string) []string {
	t.Helper()
	out := renderAllWorkloads(t, helm, append([]string{"agent.ingest.enabled=true"}, set...)...)
	return manifestcheck.Documents(out)
}

func renderedKind(t *testing.T, doc string) string {
	t.Helper()
	var o struct {
		Kind string `json:"kind"`
	}
	if err := yaml.Unmarshal([]byte(doc), &o); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, doc)
	}
	return o.Kind
}

// agentIngestServices returns every rendered Service named <release>-agent-ingest.
func agentIngestServices(t *testing.T, docs []string) []corev1.Service {
	t.Helper()
	var svcs []corev1.Service
	for _, doc := range docs {
		if renderedKind(t, doc) != "Service" {
			continue
		}
		var s corev1.Service
		if err := yaml.Unmarshal([]byte(doc), &s); err != nil {
			t.Fatalf("unmarshal Service: %v\n%s", err, doc)
		}
		if s.Name == "kubescrape-agent-ingest" {
			svcs = append(svcs, s)
		}
	}
	return svcs
}

// podWorkloads returns the pod-template labels of every rendered workload, by
// workload name, plus the agent DaemonSet itself.
func podWorkloads(t *testing.T, docs []string) (map[string]map[string]string, appsv1.DaemonSet) {
	t.Helper()
	labels := map[string]map[string]string{}
	var ds appsv1.DaemonSet
	for _, doc := range docs {
		switch renderedKind(t, doc) {
		case "DaemonSet":
			if err := yaml.Unmarshal([]byte(doc), &ds); err != nil {
				t.Fatalf("unmarshal DaemonSet: %v", err)
			}
			labels[ds.Name] = ds.Spec.Template.Labels
		case "Deployment", "StatefulSet":
			var o struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Spec struct {
					Template struct {
						Metadata struct {
							Labels map[string]string `json:"labels"`
						} `json:"metadata"`
					} `json:"template"`
				} `json:"spec"`
			}
			if err := yaml.Unmarshal([]byte(doc), &o); err != nil {
				t.Fatalf("unmarshal workload: %v", err)
			}
			labels[o.Metadata.Name] = o.Spec.Template.Metadata.Labels
		}
	}
	if ds.Name == "" {
		t.Fatal("no DaemonSet rendered")
	}
	return labels, ds
}

func agentContainer(t *testing.T, ds appsv1.DaemonSet) corev1.Container {
	t.Helper()
	for _, c := range ds.Spec.Template.Spec.Containers {
		if c.Name == "agent" {
			return c
		}
	}
	t.Fatalf("DaemonSet %s has no `agent` container", ds.Name)
	return corev1.Container{}
}

func selects(selector, podLabels map[string]string) bool {
	for k, v := range selector {
		if podLabels[k] != v {
			return false
		}
	}
	return len(selector) > 0
}

func TestIngestServiceKeepsPushesOnTheSendersNode(t *testing.T) {
	helm := helmBin(t)

	docs := ingestAgentRender(t, helm, "agent.ingest.service.enabled=true")
	svcs := agentIngestServices(t, docs)
	if len(svcs) != 1 {
		t.Fatalf("rendered %d kubescrape-agent-ingest Services, want 1", len(svcs))
	}
	svc := svcs[0]
	if svc.Namespace != "monitoring" {
		t.Errorf("Service in namespace %q, want the release namespace", svc.Namespace)
	}
	// THE property. A Cluster-policy (default) Service over a DaemonSet
	// round-robins pushes to other nodes' agents.
	if p := svc.Spec.InternalTrafficPolicy; p == nil || *p != corev1.ServiceInternalTrafficPolicyLocal {
		t.Errorf("internalTrafficPolicy = %v, want Local: without it this is the round-robin ClusterIP the docs tell operators not to build", p)
	}

	workloads, ds := podWorkloads(t, docs)
	if !selects(svc.Spec.Selector, ds.Spec.Template.Labels) {
		t.Errorf("Service selector %v does not select the agent DaemonSet's pods %v", svc.Spec.Selector, ds.Spec.Template.Labels)
	}
	// The events singleton and the trace tier run the same binary; a selector
	// matching them would hand a push to a pod with no ingest listener.
	for _, name := range slices.Sorted(maps.Keys(workloads)) {
		if name != ds.Name && selects(svc.Spec.Selector, workloads[name]) {
			t.Errorf("Service selector %v also selects %s's pods %v", svc.Spec.Selector, name, workloads[name])
		}
	}

	// Every Service port must land on a port the agent container declares, by
	// name, with the same number the listener binds.
	declared := map[string]int32{}
	for _, p := range agentContainer(t, ds).Ports {
		declared[p.Name] = p.ContainerPort
	}
	want := map[string]int32{"otlp-grpc": 4317, "otlp-http": 4318}
	got := map[string]int32{}
	for _, p := range svc.Spec.Ports {
		got[p.Name] = p.Port
		tp := p.TargetPort.String()
		n, ok := declared[tp]
		if !ok {
			t.Errorf("Service port %s targets %q, which the agent container does not declare (declared: %v)", p.Name, tp, declared)
		} else if n != p.Port {
			t.Errorf("Service port %s is %d but its target %q is container port %d", p.Name, p.Port, tp, n)
		}
	}
	if !maps.Equal(got, want) {
		t.Errorf("Service ports %v, want %v (one per ingest endpoint)", got, want)
	}

	// Off unless asked for, and never without a listener behind it: a Service
	// with an empty `ports` list is refused by the API server.
	for _, tc := range []struct {
		name string
		set  []string
	}{
		{"service not enabled", nil},
		{"ingest disabled", []string{"agent.ingest.enabled=false", "agent.ingest.service.enabled=true"}},
		{"both endpoints blanked", []string{"agent.ingest.service.enabled=true", "agent.ingest.grpcEndpoint=", "agent.ingest.httpEndpoint="}},
	} {
		if n := len(agentIngestServices(t, ingestAgentRender(t, helm, tc.set...))); n != 0 {
			t.Errorf("%s: rendered %d kubescrape-agent-ingest Services, want 0", tc.name, n)
		}
	}

	// One endpoint blanked renders the other alone.
	if svcs := agentIngestServices(t, ingestAgentRender(t, helm, "agent.ingest.service.enabled=true", "agent.ingest.grpcEndpoint=")); len(svcs) != 1 ||
		len(svcs[0].Spec.Ports) != 1 || svcs[0].Spec.Ports[0].Name != "otlp-http" {
		t.Errorf("with gRPC blanked, want one Service exposing otlp-http alone, got %+v", svcs)
	}
}

func TestIngestHostPortBindsBothListenersOnTheNode(t *testing.T) {
	helm := helmBin(t)
	for _, tc := range []struct {
		name     string
		set      []string
		wantHost bool
	}{
		{"hostPort off (default)", nil, false},
		{"hostPort on", []string{"agent.ingest.hostPort=true"}, true},
	} {
		_, ds := podWorkloads(t, ingestAgentRender(t, helm, tc.set...))
		seen := map[string]bool{}
		for _, p := range agentContainer(t, ds).Ports {
			if p.Name != "otlp-grpc" && p.Name != "otlp-http" {
				if p.HostPort != 0 {
					t.Errorf("%s: non-ingest port %s carries hostPort %d", tc.name, p.Name, p.HostPort)
				}
				continue
			}
			seen[p.Name] = true
			switch {
			case tc.wantHost && p.HostPort != p.ContainerPort:
				t.Errorf("%s: port %s hostPort %d, want it equal to containerPort %d (the downward-API recipe addresses $(NODE_IP):<the listener's port>)",
					tc.name, p.Name, p.HostPort, p.ContainerPort)
			case !tc.wantHost && p.HostPort != 0:
				t.Errorf("%s: port %s carries hostPort %d; it claims the port on EVERY node and must be opt-in", tc.name, p.Name, p.HostPort)
			}
		}
		if len(seen) != 2 {
			t.Errorf("%s: agent container declares ingest ports %v, want otlp-grpc and otlp-http", tc.name, slices.Sorted(maps.Keys(seen)))
		}
	}
}
