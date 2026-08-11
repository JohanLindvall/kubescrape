package azurediag

// Resolving the -azure-eventhub-* flags into the list of consumers to run.
//
// One CONSUMER is one kgo client: one namespace, one credential, one topic
// set, one consumer group. Several hubs share a consumer whenever one
// credential covers them all (a namespace-scoped connection string, or
// managed identity with a namespace-wide role) — that is the cheap shape and
// stays the default. What forces SEPARATE consumers is a separate CREDENTIAL:
// a Kafka connection authenticates once, so N entity-scoped connection
// strings are N clients however few hubs they name.

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
)

// SourceSpec is the flag-level description of what to consume, before it is
// resolved into concrete consumers.
type SourceSpec struct {
	// Namespaces are the Event Hubs namespace hosts. With connection strings
	// at most one may be given, as the override for their Endpoint; on the
	// managed-identity path each becomes its own consumer.
	Namespaces []string
	// Topics are the hubs to consume, shared by every consumer that does not
	// derive its own from an entity-scoped connection string. Empty consumes
	// every hub matching ^insights-.*.
	Topics []string
	// Group is the consumer group; see disambiguateGroups for what happens
	// when several consumers in ONE namespace would share it.
	Group string
	// Start is where a group with no committed offsets begins.
	Start string
	// ConnectionStringFiles select SASL PLAIN — ONE CONSUMER PER FILE, since
	// a connection authenticates with exactly one credential. Empty selects
	// managed identity.
	ConnectionStringFiles []string
	// ClientID / TenantID override the managed-identity environment.
	ClientID, TenantID string
}

// ResolveSources expands the spec into the consumers to run, each fully
// resolved (brokers, TLS, SASL mechanism, topics, group).
func ResolveSources(spec SourceSpec, log *slog.Logger) ([]KafkaConfig, error) {
	if log == nil {
		log = slog.Default()
	}
	namespaces := nonEmpty(spec.Namespaces)
	files := nonEmpty(spec.ConnectionStringFiles)

	var out []KafkaConfig
	switch {
	case len(files) > 0:
		// A namespace given alongside connection strings is the OVERRIDE for
		// their Endpoint (the single-file behaviour this generalizes). More
		// than one cannot be matched to a file without inventing a
		// positional rule nothing in the flag surface suggests, so it is
		// refused rather than guessed at.
		override := ""
		if len(namespaces) > 1 {
			return nil, fmt.Errorf("-azure-eventhub-namespace lists %d namespaces alongside %d connection strings: a connection string names its own namespace in its Endpoint, so give at most one namespace (as an override) or none",
				len(namespaces), len(files))
		}
		if len(namespaces) == 1 {
			override = namespaces[0]
		}
		for _, f := range files {
			k := KafkaConfig{
				Namespace:            override,
				Topics:               slices.Clone(spec.Topics),
				Group:                spec.Group,
				Start:                spec.Start,
				ConnectionStringFile: f,
			}
			if err := k.Resolve(log); err != nil {
				return nil, err
			}
			out = append(out, k)
		}
	case len(namespaces) > 0:
		for _, ns := range namespaces {
			k := KafkaConfig{
				Namespace: ns,
				Topics:    slices.Clone(spec.Topics),
				Group:     spec.Group,
				Start:     spec.Start,
				ClientID:  spec.ClientID,
				TenantID:  spec.TenantID,
			}
			if err := k.Resolve(log); err != nil {
				return nil, err
			}
			out = append(out, k)
		}
	default:
		return nil, fmt.Errorf("azure event hubs: set -azure-eventhub-namespace or -azure-eventhub-connection-string-file")
	}

	// An explicit topic list applies to EVERY credential, which is meaningful
	// for several namespace-scoped ones (the same hubs in each namespace) and
	// self-defeating for several entity-scoped ones, where it overrides the
	// EntityPath each string names and collapses them onto one subscription.
	// Only reading the files tells those apart, so the shape cannot be refused
	// up front — but it is the likeliest way to reach a duplicate, so the
	// refusal says so instead of leaving "identical sources" to be puzzled out.
	hint := ""
	if len(files) > 0 && len(spec.Topics) > 0 {
		hint = " — note that -azure-eventhub-topics applies to EVERY connection string and overrides the EntityPath each one names; drop it to let each string consume its own hub"
	}
	if err := rejectDuplicateSources(out, hint); err != nil {
		return nil, err
	}
	disambiguateGroups(out, log)
	return out, nil
}

// SourceName identifies this consumer in a log line or a readiness gate:
// the namespace host and what it consumes there. Stable across restarts, so
// a pending gate names the same hub every time.
func (k KafkaConfig) SourceName() string {
	ns := "?"
	if len(k.Brokers) > 0 {
		ns = hostOnly(k.Brokers[0])
	}
	if len(k.Topics) == 0 {
		return ns
	}
	return ns + "/" + strings.Join(k.Topics, ",")
}

// nonEmpty drops blank entries, so a trailing comma or an unset chart value
// rendering to "" never becomes a consumer with an empty namespace or an
// unreadable "" path.
func nonEmpty(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// sourceKey identifies what a consumer consumes: its broker set plus its
// topic set. Two consumers agreeing on both are the same subscription.
func sourceKey(k KafkaConfig) string {
	return strings.Join(k.Brokers, ",") + "\x00" + topicKey(k)
}

// topicKey is a consumer's topic set in a canonical form. Empty (the
// ^insights-.* default) is spelled out rather than left blank so it cannot
// collide with a hub legitimately named "".
func topicKey(k KafkaConfig) string {
	if len(k.Topics) == 0 {
		return "insights"
	}
	t := slices.Clone(k.Topics)
	slices.Sort(t)
	return strings.Join(t, "_")
}

// rejectDuplicateSources refuses two consumers with the same brokers AND the
// same topics. They would be two members of one group splitting the same
// partitions — which is legal Kafka and silently HALVES nothing, but it is
// never what an operator meant by listing something twice, and the likely
// cause (the same connection string mounted under two keys) is worth naming.
// hint carries the likeliest CAUSE when the caller can name one (see the call
// site); it is appended to the refusal so the message points at the fix.
func rejectDuplicateSources(ks []KafkaConfig, hint string) error {
	seen := map[string]bool{}
	for _, k := range ks {
		key := sourceKey(k)
		if seen[key] {
			return fmt.Errorf("azure event hubs: two consumers resolve to the same namespace %v and topics %v — remove the duplicate connection string or namespace%s",
				k.Brokers, k.Topics, hint)
		}
		seen[key] = true
	}
	return nil
}

// disambiguateGroups gives each consumer in a namespace its own group when the
// consumers there do NOT all consume the same topics.
//
// Event Hubs' Kafka consumer groups span a NAMESPACE (Microsoft's Kafka FAQ
// says so in as many words, and they are autocreated), so several consumers
// sharing a group name there are one group. That is exactly right when they
// share a subscription (members splitting partitions), and a trap when they
// do not: the group LEADER computes the assignment from the union of the
// members' subscriptions, using ITS OWN metadata — and an entity-scoped
// credential is not authorized for the other members' hubs, so it sees no
// partitions for them and assigns none. The starved member looks like a hub
// with no traffic. Distinct groups make that unreachable.
//
// Do not be tempted to rely on a shared group "working anyway" because a
// topic-level key is currently allowed to touch another topic's group: in
// azure-event-hubs-for-kafka#95 a Microsoft engineer called that missing
// enforcement a defect and said they would look into blocking it. Anything
// resting on it is resting on a bug they intend to fix.
//
// The suffix is applied ONLY where it is needed, which keeps the plain group
// name for every shape that works: one consumer, several consumers of the
// same topics, and consumers in DIFFERENT namespaces (whose groups cannot
// collide in the first place). The name is derived from the topic set rather
// than an index, so it is stable when the list is reordered or added to.
//
// Note for operators, documented in CONFIGURATION.md: growing a namespace
// from one consumer to several DOES rename the first one's group, and a
// group's committed offsets are the resume position — so that transition
// restarts consumption per -azure-start rather than resuming.
func disambiguateGroups(ks []KafkaConfig, log *slog.Logger) {
	byNamespace := map[string][]int{}
	for i, k := range ks {
		ns := strings.Join(k.Brokers, ",")
		byNamespace[ns] = append(byNamespace[ns], i)
	}
	for ns, idx := range byNamespace {
		if len(idx) < 2 {
			continue
		}
		// Every consumer here necessarily has a DIFFERENT topic set: a
		// namespace fixes the brokers, so two with the same topics would have
		// the same sourceKey and rejectDuplicateSources (which runs first)
		// would already have refused them. There is therefore no
		// same-subscription case to exempt — if that refusal is ever relaxed,
		// this needs the exemption back, because members deliberately sharing
		// one subscription SHOULD share a group.
		for _, i := range idx {
			base := ks[i].Group
			ks[i].Group = base + "." + topicKey(ks[i])
			log.Info("azure event hubs: giving this consumer its own group — several consumers in one namespace consume different hubs, and a shared group would let the group leader starve them",
				"namespace", ns, "topics", ks[i].Topics, "group", ks[i].Group, "configuredGroup", base)
		}
	}
}
