package cli

import (
	"flag"
	"testing"
)

// Cross-binary parity is structural now — both mains call the same
// registration, so a default or help edit cannot reach one binary and not the
// other. What CAN still regress is the set itself: a flag renamed or dropped
// here vanishes from BOTH binaries at once, and internal/manifestcheck only
// guards the flags the shipped manifests happen to pass. Pin the names and
// defaults; docs/FLAGS.md then pins the rest (it is generated from these
// registrations).
func TestSharedFlagBlocksRegisterThePinnedSet(t *testing.T) {
	fs := flag.NewFlagSet("shared", flag.ContinueOnError)
	RegisterOTLPFlags(fs, "endpoint help")
	RegisterObsFlags(fs, "agent", "the debug/health surface")

	want := map[string]string{ // name -> default, as flag.DefValue renders it
		"otlp-endpoint":                 "otel-collector.monitoring:4317",
		"otlp-protocol":                 "grpc",
		"otlp-compression":              "gzip",
		"otlp-compression-level":        "0",
		"otlp-insecure":                 "true",
		"otlp-tls-insecure-skip-verify": "false",
		"otlp-tls-ca-file":              "",
		"otlp-bearer-token-file":        "",
		"otlp-timeout":                  "15s",
		"metrics-listen":                ":9090",
		"pprof-listen":                  "",
		"self-metrics-interval":         "1m0s",
		"log-level":                     "info",
		"log-format":                    "text",
	}
	for name, def := range want {
		f := fs.Lookup(name)
		if f == nil {
			t.Errorf("shared flag -%s is not registered", name)
			continue
		}
		if f.DefValue != def {
			t.Errorf("-%s default = %q, want %q", name, f.DefValue, def)
		}
	}
	n := 0
	fs.VisitAll(func(f *flag.Flag) {
		n++
		if _, ok := want[f.Name]; !ok {
			t.Errorf("unexpected shared flag -%s; add it to the pinned set (and to docs) deliberately", f.Name)
		}
	})
	if n != len(want) {
		t.Errorf("registered %d shared flags, want %d", n, len(want))
	}
}
