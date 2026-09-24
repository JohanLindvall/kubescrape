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

// The regex default's group suffix is spelled "insights", which is also a legal
// hub name — and the IDENTITY used to be built from that suffix, so a
// namespace-scoped string (the ^insights-.* regex) beside an entity-scoped one
// for a hub literally named `insights` was refused as a duplicate, although
// they are different subscriptions and the regex does not even match that hub.
// Past the refusal both would have got group "g.insights". They must resolve,
// into two groups; the explicit hub keeps the plain derivation.
func TestResolveSourcesDefaultBesideAHubNamedInsightsIsNotADuplicate(t *testing.T) {
	const ns = "ns.servicebus.windows.net"
	got, err := ResolveSources(SourceSpec{
		Group: "g",
		ConnectionStringFiles: []string{
			writeCS(t, namespaceCS(ns)),
			writeCS(t, entityCS(ns, "insights")),
		},
	}, discardLog())
	if err != nil {
		t.Fatalf("a regex-default consumer and a hub named `insights` were refused: %v", err)
	}
	want := []string{
		ns + ":9093  g." + regexGroupSuffix, // no topics: the ^insights-.* default
		ns + ":9093 insights g.insights",
	}
	if !slices.Equal(brokersOf(got), want) {
		t.Fatalf("resolved:\n  %v\nwant:\n  %v", brokersOf(got), want)
	}
}

// sourceKey is an identity, so it must be injective over every shape a consumer
// can take — while topicKey, the readable wire-visible group suffix, keeps its
// historical spelling and is allowed to alias.
func TestSourceKeyIsInjective(t *testing.T) {
	b := []string{"ns.servicebus.windows.net:9093"}
	for _, tc := range []struct{ a, c []string }{
		{nil, []string{"insights"}},           // the regex default vs a hub named `insights`
		{[]string{"a_b"}, []string{"a", "b"}}, // a hub with an underscore vs two hubs
		{nil, []string{defaultTopicPattern}},  // the default vs a hub spelled like the pattern
		{[]string{"ab"}, []string{"a", "b"}},  // concatenation
	} {
		ka, kc := KafkaConfig{Brokers: b, Topics: tc.a}, KafkaConfig{Brokers: b, Topics: tc.c}
		if sourceKey(ka) == sourceKey(kc) {
			t.Errorf("sourceKey(%q) == sourceKey(%q): two different subscriptions share an identity", tc.a, tc.c)
		}
	}
	if sourceKey(KafkaConfig{Brokers: b, Topics: []string{"x", "y"}}) != sourceKey(KafkaConfig{Brokers: b, Topics: []string{"y", "x"}}) {
		t.Error("sourceKey depends on topic order; the same set is the same subscription")
	}
}

// Two consumers whose readable suffixes still collide after the regex default
// is moved aside must be REFUSED, never handed one group: a shared group with
// different subscriptions is the leader starvation disambiguateGroups exists to
// prevent. (Not reachable through ResolveSources today — an explicit topic list
// applies to every consumer — so it is driven directly.)
func TestDisambiguateGroupsRefusesASharedSuffix(t *testing.T) {
	b := []string{"ns.servicebus.windows.net:9093"}
	ks := []KafkaConfig{
		{Brokers: b, Topics: []string{"a_b"}, Group: "g"},
		{Brokers: b, Topics: []string{"a", "b"}, Group: "g"},
	}
	if err := disambiguateGroups(ks, discardLog()); err == nil || !strings.Contains(err.Error(), "would share the consumer group") {
		t.Fatalf("err = %v, want a refusal naming the shared group", err)
	}
}

// A namespace-scoped string's ^insights-.* regex already reads every insights-*
// hub in that namespace, so an entity-scoped string for one of them beside it
// is a second consumer, in a second group, of the same hub: every record
// exported twice, forever, with no counter and no line. That is refused,
// naming both files; a mixed pair whose entity the regex does NOT match stays
// legitimate (TestResolveSourcesMixedScopesGetDistinctGroups).
func TestResolveSourcesRefusesAHubTheRegexAlreadyReads(t *testing.T) {
	const ns = "ns.servicebus.windows.net"
	nsFile, entityFile := writeCS(t, namespaceCS(ns)), writeCS(t, entityCS(ns, "insights-logs-audit"))
	_, err := ResolveSources(SourceSpec{
		Group:                 "g",
		ConnectionStringFiles: []string{nsFile, entityFile},
	}, discardLog())
	if err == nil {
		t.Fatal("a hub consumed by both the regex default and an entity-scoped string was accepted")
	}
	for _, want := range []string{"insights-logs-audit", nsFile, entityFile, "exported twice"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
}

// More than one namespace alongside connection strings cannot be matched to a
// file without inventing a positional rule, so it is refused rather than
// guessed at.
func TestResolveSourcesRefusesManyNamespacesWithConnectionStrings(t *testing.T) {
	spec := SourceSpec{
		Namespaces:            []string{"a.servicebus.windows.net", "b.servicebus.windows.net"},
		ConnectionStringFiles: []string{writeCS(t, entityCS("c.servicebus.windows.net", "h"))},
		Group:                 "g",
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

	if _, err := ResolveSources(SourceSpec{Namespaces: []string{"", " "}, Group: "g"}, discardLog()); err == nil {
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
	if _, err := ResolveSources(SourceSpec{Namespaces: []string{"\t"}, Group: "g"}, discardLog()); err == nil {
		t.Fatal("a whitespace-only namespace resolved to a consumer")
	}
}

// Two flag shapes that are decidable from the flags alone used to pass
// -check-config and then never connect. An EMPTY consumer group (the chart
// renders the flag unconditionally, and its schema allows "") is refused by kgo
// only when the consumer is opened — a Warn per backoff and a readiness gate
// that never clears. A namespace given as the portal's Endpoint value
// (sb://myns.servicebus.windows.net/) was normalised on the connection-string
// door only: on the flag its ':' suppressed the :9093 append, the raw URL became
// the seed broker, and the managed-identity audience host came out as "sb".
func TestResolveSourcesRefusesAnEmptyGroupAndTakesThePortalEndpoint(t *testing.T) {
	for _, group := range []string{"", "  "} {
		_, err := ResolveSources(SourceSpec{Namespaces: []string{"ns.servicebus.windows.net"}, Group: group}, discardLog())
		if err == nil || !strings.Contains(err.Error(), "-azure-eventhub-group") {
			t.Errorf("group %q: err = %v, want a refusal naming -azure-eventhub-group", group, err)
		}
		if ValidateGroup(group) == nil {
			t.Errorf("ValidateGroup(%q) accepted an empty group; -check-config calls it", group)
		}
	}
	if err := ValidateGroup("$Default"); err != nil {
		t.Errorf("ValidateGroup($Default) = %v", err)
	}

	for _, ns := range []string{
		"sb://myns.servicebus.windows.net/",
		"amqps://myns.servicebus.windows.net",
		" myns.servicebus.windows.net/ ",
	} {
		got, err := ResolveSources(SourceSpec{Namespaces: []string{ns}, Group: "g"}, discardLog())
		if err != nil {
			t.Fatalf("%q: %v", ns, err)
		}
		if len(got) != 1 || !slices.Equal(got[0].Brokers, []string{"myns.servicebus.windows.net:9093"}) {
			t.Errorf("%q resolved to %v, want brokers [myns.servicebus.windows.net:9093]", ns, brokersOf(got))
		}
	}
	// And as the OVERRIDE beside a connection string, the other door the flag
	// reaches.
	got, err := ResolveSources(SourceSpec{
		Namespaces:            []string{"sb://myns.servicebus.windows.net/"},
		ConnectionStringFiles: []string{writeCS(t, namespaceCS("other.servicebus.windows.net"))},
		Group:                 "g",
	}, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !slices.Equal(got[0].Brokers, []string{"myns.servicebus.windows.net:9093"}) {
		t.Errorf("override resolved to %v, want brokers [myns.servicebus.windows.net:9093]", brokersOf(got))
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
