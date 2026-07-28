package azurediag

// The franz-go consumer behind the source interface. Event Hubs' Kafka
// surface (namespace host, port 9093, TLS) is plain Kafka to the client;
// the consumer group holds the offsets, so the "position store" is the hub
// itself and survives pod replacement without any ConfigMap.

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

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
func (k *KafkaConfig) Resolve() error {
	host := strings.TrimSpace(k.Namespace)
	if k.ConnectionStringFile != "" {
		if host == "" {
			// The connection string names the namespace; read it once here —
			// the SASL path re-reads per session, but the HOST of a rotated
			// key never changes.
			cs, err := readTrimmed(k.ConnectionStringFile)
			if err != nil {
				return err
			}
			if host = namespaceFromConnectionString(cs); host == "" {
				return fmt.Errorf("connection string in %s carries no Endpoint=sb://... to derive the namespace from", k.ConnectionStringFile)
			}
		}
		k.Mechanism = connectionStringMechanism(k.ConnectionStringFile)
	} else {
		if host == "" {
			return fmt.Errorf("azure event hubs: set -azure-eventhub-namespace or -azure-eventhub-connection-string-file")
		}
		k.Mechanism = managedIdentitySource(strings.TrimSuffix(hostOnly(host), ":9093"), k.ClientID, k.TenantID, nil).mechanism()
	}
	if !strings.Contains(host, ":") {
		host += ":9093"
	}
	k.Brokers = []string{host}
	k.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return nil
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
}

func newKafkaSource(cfg *Config) (source, error) {
	k := &cfg.Kafka
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
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("building the kafka client: %w", err)
	}
	return &kafkaSource{cl: cl, log: cfg.Logger}, nil
}

// poll blocks for the next fetch and returns the message values. Partition
// errors are counted and logged but do not fail the poll — the fetched
// records are still processed (kgo retries the broken partitions itself).
func (s *kafkaSource) poll(ctx context.Context) ([][]byte, error) {
	fetches := s.cl.PollFetches(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, fe := range fetches.Errors() {
		obs.AzureFetchErrors.Inc()
		s.log.Warn("event hubs fetch error", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
	}
	var out [][]byte
	fetches.EachRecord(func(rec *kgo.Record) {
		if len(rec.Value) > 0 {
			out = append(out, rec.Value)
		}
	})
	return out, nil
}

func (s *kafkaSource) commit(ctx context.Context) error {
	return s.cl.CommitUncommittedOffsets(ctx)
}

func (s *kafkaSource) close() { s.cl.Close() }
