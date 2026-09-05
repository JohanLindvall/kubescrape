package azurediag

// The franz-go consumer behind the source interface. Event Hubs' Kafka
// surface (namespace host, port 9093, TLS) is plain Kafka to the client;
// the consumer group holds the offsets, so the "position store" is the hub
// itself and survives pod replacement without any ConfigMap.

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// KafkaConfig is the connection half of Config. Production fills Namespace
// and the auth fields (Resolve derives brokers/TLS/SASL); tests fill Brokers
// directly and leave TLSConfig/Mechanism nil for a plaintext broker.
type KafkaConfig struct {
	// Namespace is the Event Hubs namespace host
	// (myns.servicebus.windows.net[:9093]).
	Namespace string
	// Topics are the hubs to consume; empty consumes every hub matching
	// ^insights- — the names diagnostic settings create by default.
	Topics []string
	// Group is the consumer group (default "$Default", the one every
	// namespace is born with).
	Group string
	// Start is where a group with NO committed offsets begins: end | start.
	Start string

	// ConnectionStringFile selects SASL PLAIN with the file's connection
	// string; empty selects managed identity (OAUTHBEARER).
	ConnectionStringFile string
	// ClientID / TenantID override $AZURE_CLIENT_ID / $AZURE_TENANT_ID for
	// the managed-identity path.
	ClientID, TenantID string

	// Resolved connectivity (tests fill these directly).
	Brokers   []string
	TLSConfig *tls.Config
	Mechanism sasl.Mechanism
}

// Resolve derives Brokers/TLSConfig/Mechanism from the Azure-facing fields.
func (k *KafkaConfig) Resolve(log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	host := strings.TrimSpace(k.Namespace)
	if k.ConnectionStringFile != "" {
		// Read once here — the SASL path re-reads per session, but neither the
		// HOST nor the ENTITY of a rotated key changes.
		cs, err := readTrimmed(k.ConnectionStringFile)
		if err != nil {
			return err
		}
		parsed := parseConnectionString(cs)
		if host == "" {
			if host = parsed.Namespace; host == "" {
				return fmt.Errorf("connection string in %s carries no Endpoint=sb://... to derive the namespace from", k.ConnectionStringFile)
			}
		}
		k.applyEntityPath(parsed.EntityPath, log)
		k.Mechanism = connectionStringMechanism(k.ConnectionStringFile)
	} else {
		if host == "" {
			return errors.New("azure event hubs: set -azure-eventhub-namespace or -azure-eventhub-connection-string-file")
		}
		k.Mechanism = managedIdentitySource(strings.TrimSuffix(hostOnly(host), ":9093"), k.ClientID, k.TenantID, nil, log).mechanism()
	}
	if !strings.Contains(host, ":") {
		host += ":9093"
	}
	k.Brokers = []string{host}
	k.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return nil
}

// applyEntityPath honours an ENTITY-SCOPED connection string's EntityPath.
//
// Such a string (a shared access policy copied from one Event Hub rather than
// from the namespace) both NAMES the hub and BOUNDS the credential: Azure
// documents SAS scope as the resource the rule was created on, so an
// entity-level rule "enforces granular access to topic1 only". That makes the
// default topic selection wrong for two reasons of very different strength:
//
//   - CERTAIN: the default is the `^insights-.*` pattern, and an entity is
//     rarely named `insights-...` — yours is whatever the diagnostic setting
//     called it. The pattern matches nothing, so the consumer joins the group
//     and then consumes from zero topics, which is indistinguishable from a
//     hub with no traffic.
//   - CONTESTED, and sidestepped rather than resolved: the pattern is a REGEX
//     subscription, which makes kgo issue a metadata request for the WHOLE
//     namespace (empty topic list). azure-event-hubs-for-kafka#159 — open
//     since 2021, past a fix Microsoft announced as rolling out — has the
//     broker force-closing exactly that request for an entity-level identity,
//     surfacing as a TCP reset ("run out of available brokers"), not as an
//     error code. That report is OAuth; one tester says SAS did not reproduce
//     it, while knative#2692 hits the same signature with an entity-scoped SAS
//     key. Nobody has published the entity-scoped-SAS-plus-regex case either
//     way. Naming the topic means never asking the question.
//
// Naming the entity explicitly avoids both. An EXPLICIT
// -azure-eventhub-topics still wins (explicit configuration beats a derived
// default), but a list that does not contain the entity can only end in a
// topic-authorization error per fetch, so it is warned about at startup where
// it is diagnosable rather than left to the fetch-error log.
func (k *KafkaConfig) applyEntityPath(entity string, log *slog.Logger) {
	if entity == "" {
		return
	}
	if len(k.Topics) == 0 {
		k.Topics = []string{entity}
		log.Info("azure event hubs: consuming the hub the connection string's EntityPath names",
			"topic", entity)
		return
	}
	if !slices.Contains(k.Topics, entity) {
		log.Warn("azure event hubs: the connection string is scoped to one hub that the configured topics do not include — fetches will fail authorization",
			"entityPath", entity, "topics", k.Topics)
	}
}

func hostOnly(h string) string {
	if i := strings.IndexByte(h, ':'); i >= 0 {
		return h[:i]
	}
	return h
}

func readTrimmed(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", path, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// kafkaSource wraps a kgo client as a source.
type kafkaSource struct {
	cl  *kgo.Client
	log *slog.Logger
	// fetchWarn throttles the per-fetch error line by (topic, partition). An
	// identity without read permission on one hub of a namespace produces the
	// SAME error on every poll for as long as the deployment lives, and the
	// useful information in it is one line per hub — logdedupe's whole subject.
	// The bound is per-partition keys, and saturation SUPPRESSES: a namespace
	// wide enough to fill it has already told the operator what to fix.
	fetchWarn *logdedupe.Table
}

// fetchWarnKeys / fetchWarnEvery bound the fetch-error throttle: enough keys for
// a large namespace's partitions, re-warned every five minutes so an operator
// who fixes the permission sees it stop.
const (
	fetchWarnKeys  = 64
	fetchWarnEvery = 5 * time.Minute
)

// assignedPartitions is how many Event Hubs partitions this process's
// consumers currently own, summed over every source; kubescrape_azure_partitions_assigned
// reads it (obs.RegisterAzurePartitions). Package-level because the gauge is
// one series for the process and the sources are built one at a time.
var assignedPartitions atomic.Int64

// AssignedPartitions reports the partitions currently assigned across every
// consumer this process runs — the metric's read function.
func AssignedPartitions() float64 { return float64(assignedPartitions.Load()) }

// countPartitions is the partition count of a kgo assignment map.
func countPartitions(m map[string][]int32) int {
	n := 0
	for _, ps := range m {
		n += len(ps)
	}
	return n
}

// topicList renders an assignment map's topics for a log line, sorted so two
// lines about the same set read the same.
func topicList(m map[string][]int32) string {
	return strings.Join(slices.Sorted(maps.Keys(m)), ",")
}

func newKafkaSource(cfg *Config) (source, error) {
	k := &cfg.Kafka
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(k.Brokers...),
		kgo.ClientID("kubescrape-agent"),
		kgo.ConsumerGroup(k.Group),
		// Offsets are the delivery position: committed only after the
		// collector acks (source.commit), never automatically.
		kgo.DisableAutoCommit(),
		// One poll becomes one OTLP conversion in memory; the default 50 MiB
		// fetch would balloon that.
		kgo.FetchMaxBytes(16 << 20),
		// Event Hubs closes idle Kafka connections aggressively; an
		// explicit session timeout inside its accepted window keeps group
		// membership stable across quiet hubs.
		kgo.SessionTimeout(30 * time.Second),
	}
	if len(k.Topics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(k.Topics...))
	} else {
		opts = append(opts, kgo.ConsumeTopics("^insights-.*"), kgo.ConsumeRegex())
	}
	if k.Start == StartBeginning {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}
	if k.TLSConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(k.TLSConfig))
	}
	if k.Mechanism != nil {
		opts = append(opts, kgo.SASL(k.Mechanism))
	}
	// kgo retries connection/TLS/SASL failures internally and forever, so
	// without a logger a wrong credential is a consumer that blocks in silence
	// (see kgolog.go).
	opts = append(opts, kgo.WithLogger(kgoLoggerFor(cfg.Logger)))
	// The assignment callbacks feed kubescrape_azure_partitions_assigned and
	// narrate each change at Info: a consumer that joined its group and owns
	// NOTHING (a topic pattern matching no hub, an entity-scoped credential in
	// a shared group whose leader cannot see its hub) had no signal at all —
	// the records counter reads exactly like a quiet hub — and a rebalance is
	// a lifecycle event an operator wants without asking. Lost is separate
	// from revoked: it means the group gave this member up without a handshake
	// (a missed heartbeat), which is worth a Warn where a revoke is not.
	opts = append(opts,
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			n := countPartitions(assigned)
			total := assignedPartitions.Add(int64(n))
			log.Info("event hubs partitions assigned", "group", k.Group, "topics", topicList(assigned),
				"partitions", n, "assignedTotal", total)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			n := countPartitions(revoked)
			total := assignedPartitions.Add(-int64(n))
			log.Info("event hubs partitions revoked (rebalance)", "group", k.Group, "topics", topicList(revoked),
				"partitions", n, "assignedTotal", total)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			n := countPartitions(lost)
			total := assignedPartitions.Add(-int64(n))
			log.Warn("event hubs partitions lost: the group gave this member up without a rebalance handshake; they are reassigned by the next one",
				"group", k.Group, "topics", topicList(lost), "partitions", n, "assignedTotal", total)
		}),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("building the kafka client: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	// One line per opened consumer: what it will talk to and how. The topic
	// selection in particular is derived (an EntityPath, or the ^insights-.*
	// pattern), so "which hubs am I actually consuming" is otherwise only
	// answerable from the absence of data. Never the credential — only the SASL
	// mechanism's name, which is all that may be said about one.
	log.Info("event hubs consumer opened", "brokers", strings.Join(k.Brokers, ","),
		"topics", topicSelection(k), "group", k.Group, "mechanism", describeMechanism(k),
		"startMode", startOrDefault(k.Start))
	return &kafkaSource{cl: cl, log: cfg.Logger, fetchWarn: logdedupe.New(fetchWarnKeys, fetchWarnEvery)}, nil
}

// poll blocks for the next fetch and returns the message values.
func (s *kafkaSource) poll(ctx context.Context) ([][]byte, bool, error) {
	fetches := s.cl.PollFetches(ctx)
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	return pollResult(fetches, s.log, s.fetchWarn)
}

// pollResult separates a fetch's records from its errors, classifies the
// errors by SCOPE, and reports whether the fetch was a real one.
//
// kgo delivers EVERY partition-level notification through the same record-less
// shape — addFakeReadyForDraining builds a Fetch with no records and
// PollFetches returns it alone — and Fetches.Errors' own documentation
// classifies most of those as informational and self-healing: *ErrDataLoss
// ("the client automatically resets consuming ... not worth restarting the
// client for"), ErrGroupSession (kgo rejoins by itself), and metadata load
// errors scoped to ONE topic or partition. That last shape is the live one
// here: newKafkaSource consumes ^insights-.* with ConsumeRegex, so metadata
// covers every hub in the namespace and an identity with read on nine of ten
// hubs produces exactly it. Failing the poll there makes Run close the source
// — a full LeaveGroup — and rebuild after a backoff, so nine healthy hubs go
// dark and the group rebalances twice per cycle for a permission problem on
// one.
//
// So an error naming a TOPIC never fails the poll: it is scoped to that topic,
// kgo retries it, and the rest of the namespace keeps streaming. Only a
// condition no further fetch can clear does (see fatalFetchErr).
//
// healthy is false whenever a fetch carried errors and NO records. By the
// records alone such a fetch is indistinguishable from a clean empty poll, so
// the caller must not clear the azure-eventhub readiness gate on it — that is
// how a consumer which can never consume is kept from reporting ready without
// also tearing the group down over it.
func pollResult(fetches kgo.Fetches, log *slog.Logger, warn *logdedupe.Table) (msgs [][]byte, healthy bool, err error) {
	var seen bool
	fetches.EachRecord(func(rec *kgo.Record) {
		seen = true
		if len(rec.Value) > 0 {
			msgs = append(msgs, rec.Value)
		}
	})
	errs := fetches.Errors()
	var fatal error
	for _, fe := range errs {
		obs.AzureFetchErrors.Inc()
		// A FATAL error is logged unconditionally: it closes and rebuilds the
		// client, which is a transition an operator must see every time. A
		// scoped one is a persisting state (see kafkaSource.fetchWarn).
		isFatal := fatalFetchErr(fe)
		if isFatal {
			log.Warn("event hubs fetch failed for the whole namespace; the consumer is closed and rebuilt with freshly read credentials",
				"topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
		} else if allow, saturated := warnAllow(warn, fe); allow {
			log.Warn("event hubs fetch error; kgo retries it and the rest of the namespace keeps streaming",
				"topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
			if saturated {
				// Once, on the call that fills the table: the list above is
				// truncated from here on, and an operator reading it has to know
				// that rather than infer a clean namespace.
				log.Warn("further event hubs fetch errors from other topics are suppressed; this namespace has more failing partitions than the log throttle holds",
					"maxKeys", fetchWarnKeys)
			}
		}
		if fatal == nil && isFatal {
			fatal = fmt.Errorf("event hubs fetch (topic %q partition %d): %w", fe.Topic, fe.Partition, fe.Err)
		}
	}
	if fatal != nil {
		return nil, false, fatal
	}
	return msgs, seen || len(errs) == 0, nil
}

// warnAllow asks the throttle whether this fetch error may log. A nil table
// (the direct pollResult tests) always allows: the throttle is a production
// bound, never a behaviour the caller has to construct to get output.
func warnAllow(t *logdedupe.Table, fe kgo.FetchError) (allow, saturated bool) {
	if t == nil {
		return true, false
	}
	// Keyed by topic and partition, NOT by the error text: the text is the
	// broker's and can vary per attempt (host names, correlation ids), which
	// would defeat the throttle exactly where it is needed.
	return t.Allow(fe.Topic + "/" + strconv.Itoa(int(fe.Partition)))
}

// fatalFetchErr reports a fetch error that only a NEW client can clear, which
// is the whole set Run's close-and-reopen arm can address.
//
// An error naming a topic is scoped to that topic (a metadata load failure, a
// non-retryable offset-fetch error, an injected *ErrDataLoss), so it says
// nothing about the other hubs and kgo retries it on its own. An unscoped
// ErrGroupSession is self-healing too — kgo rejoins the group, and a rebuild
// would only add a LeaveGroup to a rebalance already under way. What remains
// is the closed client (which errors.Is tests exactly as Fetches.IsClientClosed
// does, but per error, so a fetch mixing it with records is still counted and
// logged) and an unscoped NON-RETRIABLE broker error — a cluster-wide
// authorization or SASL failure, the one shape where every hub is unreachable
// and a fresh client with freshly read credentials is the only recovery.
func fatalFetchErr(fe kgo.FetchError) bool {
	if errors.Is(fe.Err, kgo.ErrClientClosed) {
		return true
	}
	if fe.Topic != "" {
		return false
	}
	if gs := (*kgo.ErrGroupSession)(nil); errors.As(fe.Err, &gs) {
		return false
	}
	ke := (*kerr.Error)(nil)
	return errors.As(fe.Err, &ke) && !ke.Retriable
}

// topicSelection renders what this client subscribes to: the explicit list, or
// the regex the default uses.
func topicSelection(k *KafkaConfig) string {
	if len(k.Topics) > 0 {
		return strings.Join(k.Topics, ",")
	}
	return "^insights-.* (regex)"
}

// startOrDefault names where a group with no committed offsets begins.
func startOrDefault(start string) string {
	if start == StartBeginning {
		return StartBeginning
	}
	return StartEnd
}

func (s *kafkaSource) commit(ctx context.Context) error {
	return s.cl.CommitUncommittedOffsets(ctx)
}

func (s *kafkaSource) close() { s.cl.Close() }
