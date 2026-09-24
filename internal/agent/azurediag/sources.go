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
	"errors"
	"fmt"
	"log/slog"
	"regexp"
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
	if err := ValidateGroup(spec.Group); err != nil {
		return nil, err
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
		return nil, errors.New("azure event hubs: set -azure-eventhub-namespace or -azure-eventhub-connection-string-file")
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
	if err := rejectRegexOverlap(out); err != nil {
		return nil, err
	}
	if err := disambiguateGroups(out, log); err != nil {
		return nil, err
	}
	return out, nil
}

// ValidateGroup refuses an empty consumer group. kgo refuses one too — beside
// the disabled autocommit this consumer runs with, every client build fails —
// but only when the consumer is OPENED, i.e. after a rollout, as a Warn per
// backoff and a readiness gate that never clears. The chart renders the group
// flag unconditionally, so `azure.eventhub.group: ""` reaches here as an
// explicit empty value rather than as the "$Default" flag default. It is a
// flag-shape check, so -check-config calls it too.
func ValidateGroup(group string) error {
	if strings.TrimSpace(group) == "" {
		return errors.New(`-azure-eventhub-group is empty: a consumer group is required (the flag's default "$Default" is the group every Event Hubs namespace is born with)`)
	}
	return nil
}

// SourceName identifies this consumer in a log line or a readiness gate:
// the namespace host and what it consumes there. Stable across restarts, so
// a pending gate names the same hub every time.
func (k *KafkaConfig) SourceName() string {
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
// subscription. Two consumers agreeing on both are the same subscription.
//
// It is an IDENTITY, so it must be injective, and it is deliberately NOT built
// from topicKey, the readable group suffix: that one spells the ^insights-.*
// default as "insights", which is also a legal hub name, and joins a topic set
// with "_", which a hub name may contain — so a namespace-scoped consumer and
// an entity-scoped one for a hub literally named `insights` were refused as
// duplicates although the regex does not even match that hub. Here the default
// is the PATTERN, behind a discriminator no explicit set can produce, and
// topics are NUL-separated (a hub name cannot contain NUL).
func sourceKey(k KafkaConfig) string {
	var b strings.Builder
	b.WriteString(strings.Join(k.Brokers, ","))
	if len(k.Topics) == 0 {
		b.WriteString("\x00regex\x00")
		b.WriteString(defaultTopicPattern)
		return b.String()
	}
	b.WriteString("\x00topics")
	t := slices.Clone(k.Topics)
	slices.Sort(t)
	for _, topic := range t {
		b.WriteByte(0)
		b.WriteString(topic)
	}
	return b.String()
}

// topicKey is a consumer's topic set in the readable form disambiguateGroups
// appends to a group name — WIRE-VISIBLE, since the group's committed offsets
// are the resume position, so it is kept exactly as it has always been rather
// than made injective: the ^insights-.* default is spelled "insights" (no hub
// can be named "", but one CAN be named `insights`, which disambiguateGroups
// resolves) and a set is joined with "_". Identity is sourceKey's job.
func topicKey(k KafkaConfig) string {
	if len(k.Topics) == 0 {
		return "insights"
	}
	t := slices.Clone(k.Topics)
	slices.Sort(t)
	return strings.Join(t, "_")
}

// defaultTopicRE is defaultTopicPattern compiled as kgo's ConsumeRegex compiles
// it (Go regexp, unanchored unless the pattern anchors itself).
var defaultTopicRE = regexp.MustCompile(defaultTopicPattern)

// rejectRegexOverlap refuses an explicit hub that a regex-default consumer on
// the SAME brokers already reads.
//
// sourceKey keeps the two apart — one is a pattern, the other a hub — and
// disambiguateGroups gives each its own group, so both are accepted and every
// record of that hub is consumed and exported TWICE, forever, with no counter
// and no line. The shape is a namespace-scoped connection string (whose empty
// topic list subscribes to ^insights-.*) beside an entity-scoped one for an
// `insights-...` hub in the same namespace: the namespace-scoped key already
// covers every hub there, so the entity-scoped string is redundant, which is
// what the refusal says. It is the regex-overlap escape from the duplicate rule
// rejectDuplicateSources states, and refused for the same reason: never what an
// operator meant. A mixed pair whose entity is NOT an insights-* hub stays
// legitimate — the regex does not read it.
func rejectRegexOverlap(ks []KafkaConfig) error {
	for _, re := range ks {
		if len(re.Topics) != 0 {
			continue
		}
		brokers := strings.Join(re.Brokers, ",")
		for _, k := range ks {
			if len(k.Topics) == 0 || strings.Join(k.Brokers, ",") != brokers {
				continue
			}
			for _, topic := range k.Topics {
				if defaultTopicRE.MatchString(topic) {
					return fmt.Errorf("azure event hubs: hub %q in %s is consumed twice — by the consumer from %s, which names it, and by the one from %s, which already reads every hub matching %s in that namespace; every record would be exported twice. Remove the redundant entity-scoped connection string: the namespace-scoped one already covers that hub",
						topic, hostOnly(brokers), describeSource(k), describeSource(re), defaultTopicPattern)
				}
			}
		}
	}
	return nil
}

// describeSource names where a consumer's credential came from, for a refusal:
// its connection-string file, or the managed identity.
func describeSource(k KafkaConfig) string {
	if k.ConnectionStringFile != "" {
		return k.ConnectionStringFile
	}
	return "the managed identity"
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
//
// The suffix is topicKey, which is readable rather than injective, so two
// DIFFERENT subscriptions can spell the same one: the regex default ("insights")
// beside an entity-scoped hub literally named `insights`. The default-pattern
// consumer then takes regexGroupSuffix instead — an `insights-` name, and a hub
// of that shape beside a regex consumer in one namespace was already refused
// (rejectRegexOverlap), so it cannot be taken — while the explicit hub keeps
// the plain derivation, as every non-colliding shape does. A suffix still
// shared after that is refused rather than handed out twice: two consumers in
// one group with different subscriptions is the starvation this function
// exists to prevent.
func disambiguateGroups(ks []KafkaConfig, log *slog.Logger) error {
	byNamespace := map[string][]int{}
	for i, k := range ks {
		ns := strings.Join(k.Brokers, ",")
		byNamespace[ns] = append(byNamespace[ns], i)
	}
	for ns, idx := range byNamespace {
		if len(idx) < 2 {
			continue
		}
		// Every consumer here necessarily has a DIFFERENT subscription: a
		// namespace fixes the brokers, so two with the same one would have
		// the same sourceKey and rejectDuplicateSources (which runs first)
		// would already have refused them. There is therefore no
		// same-subscription case to exempt — if that refusal is ever relaxed,
		// this needs the exemption back, because members deliberately sharing
		// one subscription SHOULD share a group.
		suffix := make(map[int]string, len(idx))
		explicit := map[string]bool{}
		for _, i := range idx {
			suffix[i] = topicKey(ks[i])
			if len(ks[i].Topics) > 0 {
				explicit[suffix[i]] = true
			}
		}
		for _, i := range idx {
			if len(ks[i].Topics) == 0 && explicit[suffix[i]] {
				suffix[i] = regexGroupSuffix
			}
		}
		taken := map[string]int{}
		for _, i := range idx {
			if j, dup := taken[suffix[i]]; dup {
				return fmt.Errorf("azure event hubs: the consumers for %s and %s in %s would share the consumer group %q although they consume different hubs; the group leader would starve one of them — rename a hub or split the namespace",
					ks[j].SourceName(), ks[i].SourceName(), hostOnly(ns), ks[i].Group+"."+suffix[i])
			}
			taken[suffix[i]] = i
		}
		for _, i := range idx {
			base := ks[i].Group
			ks[i].Group = base + "." + suffix[i]
			log.Info("azure event hubs: giving this consumer its own group — several consumers in one namespace consume different hubs, and a shared group would let the group leader starve them",
				"namespace", ns, "topics", ks[i].Topics, "group", ks[i].Group, "configuredGroup", base)
		}
	}
	return nil
}

// regexGroupSuffix is the group suffix the regex-default consumer takes when
// its usual one ("insights") is already an explicit hub's (disambiguateGroups).
const regexGroupSuffix = "insights-regex"
