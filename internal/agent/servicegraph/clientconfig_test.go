package servicegraph

import (
	"reflect"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

// clientConfig now builds on otlpexport.Config.TransportOnly. This pins that
// the result is byte-identical to the hand-spelled derivation it replaced: the
// transport tuning (compression, timeout, send cap) inherited from the base,
// every destination-scoped base field (endpoint, credentials, TLS material,
// headers) dropped, the shard section's own destination filled in, and the
// retry shape pinned to a single attempt.
func TestClientConfigInheritsOnlyTransport(t *testing.T) {
	base := otlpexport.Config{
		// Destination-scoped: none of these may survive into a shard client.
		Endpoint:           "otel-collector.monitoring:4317",
		Insecure:           true,
		InsecureSkipVerify: true,
		CAFile:             "/etc/collector/ca.pem",
		ClientCertFile:     "/etc/collector/tls.crt",
		ClientKeyFile:      "/etc/collector/tls.key",
		Headers:            map[string]string{"X-Scope-OrgID": "tenant-a"},
		BearerTokenFile:    "/var/run/collector-token",
		// Transport: inherited.
		Protocol:         "grpc",
		Compression:      "gzip",
		CompressionLevel: 3,
		Timeout:          9 * time.Second,
		RetryAttempts:    5,
		RetryBackoff:     2 * time.Second,
		MaxSendBytes:     123456,
	}
	cfg := ReshardConfig{
		Self:        "sg-0",
		StatefulSet: "sg",
		Replicas:    2,
		CAFile:      "/etc/shards/ca.pem",
		Headers:     map[string]string{"X-Internal": "1"},
		// The shard hop's own credential, not the collector's.
		BearerTokenFile: "/var/run/shard-token",
	}
	target := shardTarget{name: "sg-1", endpoint: "sg-1.sg.monitoring.svc:4319"}

	got := cfg.clientConfig(target, base)

	want := otlpexport.Config{
		Endpoint:           target.endpoint,
		Protocol:           "grpc",
		Insecure:           false, // a caFile asks for TLS
		InsecureSkipVerify: false,
		CAFile:             cfg.CAFile,
		Headers:            cfg.Headers,
		BearerTokenFile:    cfg.BearerTokenFile,
		Compression:        base.Compression,
		CompressionLevel:   base.CompressionLevel,
		Timeout:            base.Timeout,
		RetryAttempts:      1, // the APPLICATION owns the retry
		MaxSendBytes:       base.MaxSendBytes,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("clientConfig drifted from the pinned derivation:\n got  %+v\n want %+v", got, want)
	}
}
