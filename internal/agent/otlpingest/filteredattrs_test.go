package otlpingest

// The operator's resourceAttributes config changes what the builder RENDERS,
// never what an id RESOLVES to. sameObject, the auto-mode demotion and the
// enriched/unresolved tally read the lookup result (idResult), so a config
// like defaults: false or disable: [k8s.pod.uid] cannot flip an attribution
// decision — reading the filtered rendering made a sender labelling points
// with its own pod uid look FOREIGN, demoted it to the split path, and let the
// overwrite destroy the identity it chose.

import (
	"context"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func noDefaultsBuilder(t *testing.T) *attrs.Builder {
	t.Helper()
	off := false
	b, err := attrs.NewBuilder(&attrs.Config{Defaults: &off}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func disableUIDBuilder(t *testing.T) *attrs.Builder {
	t.Helper()
	f, err := attrs.NewFilterFromLists(nil, []string{"k8s.pod.uid"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := attrs.NewBuilder(nil, f)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A sender whose points name its OWN pod must stay on the resource path in
// auto mode regardless of what the attribute filter renders — the same
// contract TestAutoModeKeepsTheSendersOwnIdentity pins for the default config.
func TestFilteredBuilderDoesNotDemoteTheSendersOwnPoints(t *testing.T) {
	meta := &fakeMeta{
		containers: map[string]*kubemeta.ContainerMetadata{
			"cafe01": {Container: kubemeta.Container{Name: "app", ID: "containerd://cafe01"},
				Pod: kubemeta.Pod{Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1"}},
		},
		pods: map[string]*kubemeta.Pod{
			"pod-uid-1": {Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1"},
		},
	}
	for _, tc := range []struct {
		name    string
		builder *attrs.Builder
	}{
		{"defaults off", noDefaultsBuilder(t)},
		{"k8s.pod.uid disabled", disableUIDBuilder(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewEnricher(Config{Meta: meta, MetricsMode: MetricsAuto, Attrs: tc.builder})
			md := gaugeWith(
				map[string]string{"container.id": "cafe01", "service.name": "my-chosen-name"},
				map[string]string{"k8s.pod.uid": "pod-uid-1"}, // the sender's own pod, by the other id kind
			)
			out := e.EnrichMetrics(context.Background(), md)
			if n := out.ResourceMetrics().Len(); n != 1 {
				t.Fatalf("resources = %d, want 1: the filter demoted the sender to the split path", n)
			}
			if got := resAttrsOf(out)["service.name"]; got != "my-chosen-name" {
				t.Errorf("service.name = %v, want my-chosen-name: the rendering config changed an attribution decision and the sender's identity was destroyed", got)
			}
		})
	}
}

// A lookup that resolves must count enriched even when the filter renders no
// attributes for it: unresolved is the attribution-health signal, and a
// rendering config must not turn a healthy fleet into a permanent unresolved
// stream.
func TestResolvedLookupCountsEnrichedUnderFilteredBuilder(t *testing.T) {
	enriched := obs.Ingested.WithLabelValues("enriched").Value()
	unresolved := obs.Ingested.WithLabelValues("unresolved").Value()

	e := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsResource, Attrs: noDefaultsBuilder(t)})
	e.EnrichMetrics(context.Background(),
		gaugeWith(map[string]string{"container.id": "cafe01"}, nil))

	if got := obs.Ingested.WithLabelValues("enriched").Value() - enriched; got != 1 {
		t.Errorf("enriched delta = %v, want 1: the id resolved", got)
	}
	if got := obs.Ingested.WithLabelValues("unresolved").Value() - unresolved; got != 0 {
		t.Errorf("unresolved delta = %v, want 0: a resolved lookup was counted unresolved", got)
	}
}
