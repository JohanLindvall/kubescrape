package otlpexport

import (
	"strings"
	"testing"
)

// Config.Validate promises "the same checks New makes before it builds
// anything", and -check-config is built entirely on that promise. It omitted
// buildTLS's mTLS pairing rule — a pure-shape rule, two strings, no file
// touched — so a routing route (or an export override, or a shard client)
// carrying only clientCertFile passed the dry run with exit 0 and then killed
// the process at `creating OTLP exporter`, which is the one outcome
// -check-config exists to prevent.
//
// The assertion is the EQUIVALENCE, not the existence of an error: both halves
// of the pair, in both orders, must be judged identically by the dry run and by
// the constructor, and with the SAME sentence — a refusal the two paths spell
// differently is a refusal an operator cannot match to their config.
func TestValidateRefusesHalfAClientCertPair(t *testing.T) {
	// A TLS destination (Insecure false): the plaintext-material refusal must
	// not be what fails these, or the pairing rule would be untested.
	base := Config{Endpoint: "collector.example.com:4317", Protocol: "grpc"}

	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"cert without key", func() Config { c := base; c.ClientCertFile = "/tls/tls.crt"; return c }()},
		{"key without cert", func() Config { c := base; c.ClientKeyFile = "/tls/tls.key"; return c }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vErr := tc.cfg.Validate()
			if vErr == nil {
				t.Fatalf("Validate accepted %s: -check-config would exit 0 on a config that aborts the process at startup", tc.name)
			}
			c, nErr := New(tc.cfg)
			if nErr == nil {
				_ = c.Close()
				t.Fatalf("New accepted %s, so the dry run is now stricter than a start", tc.name)
			}
			if vErr.Error() != nErr.Error() {
				t.Fatalf("dry run says %q, the start says %q; one mistake must have one sentence", vErr, nErr)
			}
			if !strings.Contains(vErr.Error(), "clientKeyFile") {
				t.Fatalf("error %q does not name the field that is missing", vErr)
			}
		})
	}

	// The complete pair on the same TLS destination stays accepted — the rule
	// is about the PAIR, and refusing mTLS outright would be worse than the
	// gap it replaces.
	cert, key := writeKeyPair(t)
	ok := base
	ok.ClientCertFile, ok.ClientKeyFile = cert, key
	if err := ok.Validate(); err != nil {
		t.Fatalf("a complete client certificate pair was rejected: %v", err)
	}
	c, err := New(ok)
	if err != nil {
		t.Fatalf("New rejected a complete client certificate pair: %v", err)
	}
	_ = c.Close()
}
