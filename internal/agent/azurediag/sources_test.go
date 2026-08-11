package azurediag

// Tests for resolving the flag surface into consumers (sources.go): how many
// clients a configuration becomes, which hubs each takes, and the consumer
// groups they land in.

import (
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// An obviously fake key of the right shape (base64, '=' padded). Never put a
// real one in a test file.
const testKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

// entityCS is an entity-scoped connection string for one hub in one namespace.
func entityCS(ns, hub string) string {
	return "Endpoint=sb://" + ns + "/;SharedAccessKeyName=test;SharedAccessKey=" + testKey + ";EntityPath=" + hub
}

// namespaceCS is a namespace-scoped connection string (no EntityPath).
func namespaceCS(ns string) string {
	return "Endpoint=sb://" + ns + "/;SharedAccessKeyName=Root;SharedAccessKey=" + testKey
}

// brokersOf renders each resolved consumer as "namespace topics group", the
// three things a source list is actually about.
func brokersOf(ks []KafkaConfig) []string {
	out := make([]string, 0, len(ks))
	for _, k := range ks {
		out = append(out, strings.Join(k.Brokers, ",")+" "+strings.Join(k.Topics, ",")+" "+k.Group)
	}
	slices.Sort(out)
	return out
}

// Several ENTITY-SCOPED connection strings become one consumer each: a Kafka
// connection authenticates with exactly one credential, so N such strings are
// N clients however few hubs they name. Sharing a namespace, they must also
// land in DIFFERENT consumer groups (see disambiguateGroups).
func TestResolveSourcesMultipleEntityConnectionStrings(t *testing.T) {
	const ns = "mydiag-we-0a1b2c3d.servicebus.windows.net"
	spec := SourceSpec{
		Group: "$Default",
		ConnectionStringFiles: []string{
			writeCS(t, entityCS(ns, "azure")),
			writeCS(t, entityCS(ns, "otherhub")),
		},
	}
	got, err := ResolveSources(spec, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("resolved %d consumers, want 2 (one per credential)", len(got))
	}
	want := []string{
		ns + ":9093 azure $Default.azure",
		ns + ":9093 otherhub $Default.otherhub",
	}
	if !slices.Equal(brokersOf(got), want) {
		t.Fatalf("resolved:\n  %v\nwant:\n  %v", brokersOf(got), want)
	}
	for _, k := range got {
		if k.Mechanism == nil || k.Mechanism.Name() != "PLAIN" {
			t.Fatalf("%s: mechanism = %v, want PLAIN", k.SourceName(), k.Mechanism)
		}
	}
}

// Entity-scoped strings in DIFFERENT namespaces cannot collide — Kafka
// consumer groups are per namespace — so they keep the configured group name.
func TestResolveSourcesDistinctNamespacesKeepTheGroup(t *testing.T) {
	spec := SourceSpec{
		Group: "$Default",
		ConnectionStringFiles: []string{
			writeCS(t, entityCS("ns-we.servicebus.windows.net", "azure")),
			writeCS(t, entityCS("ns-ne.servicebus.windows.net", "azure")),
		},
	}
	got, err := ResolveSources(spec, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ns-ne.servicebus.windows.net:9093 azure $Default",
		"ns-we.servicebus.windows.net:9093 azure $Default",
	}
	if !slices.Equal(brokersOf(got), want) {
		t.Fatalf("resolved:\n  %v\nwant:\n  %v", brokersOf(got), want)
	}
}

// The managed-identity path takes a LIST of namespaces, each its own client,
// sharing the topic list. This is the per-region shape (one hub name, several
// regional namespaces).
func TestResolveSourcesManagedIdentityNamespaceList(t *testing.T) {
	spec := SourceSpec{
		Namespaces: []string{"ns-we.servicebus.windows.net", "ns-ne.servicebus.windows.net"},
		Topics:     []string{"insights-logs-audit", "insights-metrics"},
		Group:      "kubescrape",
	}
	got, err := ResolveSources(spec, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ns-ne.servicebus.windows.net:9093 insights-logs-audit,insights-metrics kubescrape",
		"ns-we.servicebus.windows.net:9093 insights-logs-audit,insights-metrics kubescrape",
	}
	if !slices.Equal(brokersOf(got), want) {
		t.Fatalf("resolved:\n  %v\nwant:\n  %v", brokersOf(got), want)
	}
	for _, k := range got {
		if k.Mechanism == nil || k.Mechanism.Name() != "OAUTHBEARER" {
			t.Fatalf("%s: mechanism = %v, want OAUTHBEARER", k.SourceName(), k.Mechanism)
		}
	}
}

// A single namespace-scoped credential consuming several hubs stays ONE
// client — the cheap shape, and the one the group name is unqualified for.
func TestResolveSourcesOneCredentialManyTopicsIsOneConsumer(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec SourceSpec
	}{
		{"connection string", SourceSpec{
			Group:                 "$Default",
			Topics:                []string{"a", "b"},
			ConnectionStringFiles: []string{writeCS(t, namespaceCS("ns.servicebus.windows.net"))},
		}},
		{"managed identity", SourceSpec{
			Group:      "$Default",
			Topics:     []string{"a", "b"},
			Namespaces: []string{"ns.servicebus.windows.net"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveSources(tc.spec, discardLog())
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"ns.servicebus.windows.net:9093 a,b $Default"}
			if !slices.Equal(brokersOf(got), want) {
				t.Fatalf("resolved:\n  %v\nwant:\n  %v", brokersOf(got), want)
			}
		})
	}
}

// Two credentials for the SAME namespace and topics are refused rather than
// run as two members of one group: in one process that only doubles the
// connections to split the same partitions between two goroutines, and the
// likely cause — one connection string mounted under two secret keys — is
// worth naming. This refusal is also what makes disambiguateGroups' "every
// consumer in a namespace has a different topic set" hold.
func TestResolveSourcesDuplicateCredentialsRefused(t *testing.T) {
	spec := SourceSpec{
		Group:  "$Default",
		Topics: []string{"shared"},
		ConnectionStringFiles: []string{
			writeCS(t, namespaceCS("ns.servicebus.windows.net")),
			writeCS(t, namespaceCS("ns.servicebus.windows.net")+";Extra=1"),
		},
	}
	_, err := ResolveSources(spec, discardLog())
	if err == nil || !strings.Contains(err.Error(), "same namespace") {
		t.Fatalf("err = %v, want a duplicate-source refusal", err)
	}
}

// An entity-scoped string and a namespace-scoped one in the same namespace
// consume different topic sets, so both are qualified — including the
// regex-default one, whose key spells out the default rather than being blank.
func TestResolveSourcesMixedScopesGetDistinctGroups(t *testing.T) {
	const ns = "ns.servicebus.windows.net"
	spec := SourceSpec{
		Group: "g",
		ConnectionStringFiles: []string{
			writeCS(t, entityCS(ns, "azure")),
			writeCS(t, namespaceCS(ns)),
		},
	}
	got, err := ResolveSources(spec, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		ns + ":9093  g.insights", // no topics: the ^insights-.* default
		ns + ":9093 azure g.azure",
	}
	if !slices.Equal(brokersOf(got), want) {
		t.Fatalf("resolved:\n  %v\nwant:\n  %v", brokersOf(got), want)
	}
}

// More than one namespace alongside connection strings cannot be matched to a
// file without inventing a positional rule, so it is refused rather than
// guessed at.
func TestResolveSourcesRefusesManyNamespacesWithConnectionStrings(t *testing.T) {
	spec := SourceSpec{
		Namespaces:            []string{"a.servicebus.windows.net", "b.servicebus.windows.net"},
		ConnectionStringFiles: []string{writeCS(t, entityCS("c.servicebus.windows.net", "h"))},
	}
	_, err := ResolveSources(spec, discardLog())
	if err == nil || !strings.Contains(err.Error(), "at most one namespace") {
		t.Fatalf("err = %v, want a refusal naming the ambiguity", err)
	}
}

// A single namespace alongside connection strings stays the OVERRIDE for
// their Endpoint — the single-file behaviour this generalizes.
func TestResolveSourcesSingleNamespaceOverridesEndpoints(t *testing.T) {
	spec := SourceSpec{
		Namespaces: []string{"override.servicebus.windows.net"},
		Group:      "g",
		ConnectionStringFiles: []string{
			writeCS(t, entityCS("ignored.servicebus.windows.net", "azure")),
		},
	}
	got, err := ResolveSources(spec, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"override.servicebus.windows.net:9093 azure g"}
	if !slices.Equal(brokersOf(got), want) {
		t.Fatalf("resolved:\n  %v\nwant:\n  %v", brokersOf(got), want)
	}
}

// Blank list entries (a trailing comma, an unset chart value) must not become
// a consumer with an empty namespace or an unreadable "" path.
func TestResolveSourcesIgnoresBlankEntries(t *testing.T) {
	got, err := ResolveSources(SourceSpec{
		Namespaces: []string{"", "ns.servicebus.windows.net", "  "},
		Group:      "g",
	}, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Brokers[0] != "ns.servicebus.windows.net:9093" {
		t.Fatalf("resolved %v, want the one real namespace", brokersOf(got))
	}

	if _, err := ResolveSources(SourceSpec{Namespaces: []string{"", " "}}, discardLog()); err == nil {
		t.Fatal("an all-blank namespace list resolved to something")
	}
}

// SourceName is what a readiness gate and a log line are keyed by, so it must
// name the namespace and what is consumed there.
func TestSourceName(t *testing.T) {
	k := KafkaConfig{Brokers: []string{"ns.servicebus.windows.net:9093"}, Topics: []string{"azure"}}
	if got := k.SourceName(); got != "ns.servicebus.windows.net/azure" {
		t.Fatalf("SourceName = %q", got)
	}
	k.Topics = nil
	if got := k.SourceName(); got != "ns.servicebus.windows.net" {
		t.Fatalf("SourceName without topics = %q", got)
	}
}

// A connection string that cannot be read fails the WHOLE resolution, naming
// the file — one unreadable secret key must not silently yield a shorter list
// of consumers than was configured.
func TestResolveSourcesUnreadableFileFails(t *testing.T) {
	good := writeCS(t, entityCS("ns.servicebus.windows.net", "azure"))
	_, err := ResolveSources(SourceSpec{
		Group:                 "g",
		ConnectionStringFiles: []string{good, "/nonexistent/connection-string"},
	}, discardLog())
	if err == nil || !strings.Contains(err.Error(), "/nonexistent/connection-string") {
		t.Fatalf("err = %v, want a failure naming the unreadable file", err)
	}
}

// The managed-identity arm refuses a namespace it cannot use, rather than
// resolving a consumer with no brokers.
func TestResolveSourcesRejectsUnusableNamespace(t *testing.T) {
	// A namespace-less spec is the only way Resolve fails on that arm; the
	// blank-entry test covers the list form, this the empty-after-trim one.
	if _, err := ResolveSources(SourceSpec{Namespaces: []string{"\t"}}, discardLog()); err == nil {
		t.Fatal("a whitespace-only namespace resolved to a consumer")
	}
}

// Every consumer in a namespace is qualified — including the one that would
// otherwise have kept the bare name — so no two of them share a group.
func TestResolveSourcesGroupsAreUniqueWithinANamespace(t *testing.T) {
	const ns = "ns.servicebus.windows.net"
	got, err := ResolveSources(SourceSpec{
		Group: "$Default",
		ConnectionStringFiles: []string{
			writeCS(t, entityCS(ns, "a")),
			writeCS(t, entityCS(ns, "b")),
			writeCS(t, entityCS(ns, "c")),
		},
	}, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, k := range got {
		if seen[k.Group] {
			t.Fatalf("two consumers share group %q in one namespace: %v", k.Group, brokersOf(got))
		}
		seen[k.Group] = true
		if k.Group == "$Default" {
			t.Fatalf("consumer %s kept the bare group name alongside siblings", k.SourceName())
		}
	}
}

// An explicit topic list applies to EVERY connection string, so with several
// ENTITY-scoped ones it overrides each string's EntityPath and collapses them
// onto one subscription — which then trips the duplicate refusal. The shape
// cannot be refused up front (the same combination is legitimate for several
// NAMESPACE-scoped strings in different namespaces), so the message must name
// the cause rather than leave "identical sources" to be puzzled out.
func TestResolveSourcesExplicitTopicsOverridingEntityPathsIsExplained(t *testing.T) {
	const ns = "ns.servicebus.windows.net"
	_, err := ResolveSources(SourceSpec{
		Group:  "g",
		Topics: []string{"azure", "otherhub"},
		ConnectionStringFiles: []string{
			writeCS(t, entityCS(ns, "azure")),
			writeCS(t, entityCS(ns, "otherhub")),
		},
	}, discardLog())
	if err == nil {
		t.Fatal("two entity strings collapsed onto one subscription without complaint")
	}
	if !strings.Contains(err.Error(), "-azure-eventhub-topics") {
		t.Fatalf("err = %v\nwant it to name the topics flag as the cause", err)
	}

	// The legitimate twin: the same flag across DIFFERENT namespaces is not a
	// duplicate at all, so it must resolve cleanly and gain no such hint.
	got, err := ResolveSources(SourceSpec{
		Group:  "g",
		Topics: []string{"insights-logs-audit"},
		ConnectionStringFiles: []string{
			writeCS(t, namespaceCS("ns-we.servicebus.windows.net")),
			writeCS(t, namespaceCS("ns-ne.servicebus.windows.net")),
		},
	}, discardLog())
	if err != nil {
		t.Fatalf("the same flag across two namespaces was refused: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("resolved %d consumers, want 2", len(got))
	}
}
