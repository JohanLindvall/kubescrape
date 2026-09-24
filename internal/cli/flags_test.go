package cli

import (
	"flag"
	"reflect"
	"testing"
)

// Cross-binary parity is structural now — both mains call the same
// registration, so a default or help edit cannot reach one binary and not the
// other. What CAN still regress is the set itself: a flag renamed or dropped
// here vanishes from BOTH binaries at once, and internal/manifestcheck only
// guards the flags the shipped manifests happen to pass. -log-format is the
// worked example: it was removed deliberately (one format, logfmt, always), and
// removing it meant fixing the chart templates that passed it in the same
// change — a flag a manifest still passes is "flag provided but not defined"
// plus exit 2, on exactly the deployments most likely to have set it. Pin the names and
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

// The startup line reports EVERY flag of the block: each main used to pick its
// own subset and the service's had dropped -otlp-compression and
// -otlp-tls-insecure-skip-verify, so a service exporting with certificate
// verification off never said so. A field added to OTLPFlags without a key
// here fails.
func TestOTLPSummaryAttrsReportEveryFlag(t *testing.T) {
	fs := flag.NewFlagSet("shared", flag.ContinueOnError)
	f := RegisterOTLPFlags(fs, "endpoint help")
	// Distinct values throughout, so a skipped field cannot hide behind a
	// sibling that happens to hold the same value.
	for name, val := range map[string]string{
		"otlp-tls-ca-file": "/ca.pem", "otlp-bearer-token-file": "/token",
		"otlp-insecure": "false", "otlp-tls-insecure-skip-verify": "true", "otlp-compression-level": "5",
	} {
		if err := fs.Set(name, val); err != nil {
			t.Fatal(err)
		}
	}
	attrs := f.SummaryAttrs()
	if len(attrs)%2 != 0 {
		t.Fatalf("SummaryAttrs is not key/value pairs: %v", attrs)
	}
	got := map[string]any{}
	for i := 0; i < len(attrs); i += 2 {
		k, ok := attrs[i].(string)
		if !ok {
			t.Fatalf("key %v is not a string", attrs[i])
		}
		if _, dup := got[k]; dup {
			t.Errorf("key %q reported twice", k)
		}
		got[k] = attrs[i+1]
	}
	v := reflect.ValueOf(*f)
	if len(got) != v.NumField() {
		t.Errorf("SummaryAttrs reports %d keys for the %d flags of OTLPFlags: %v", len(got), v.NumField(), got)
	}
	// Every flag's value appears under some key, so no field is silently
	// skipped.
	for i := 0; i < v.NumField(); i++ {
		want := v.Field(i).Elem().Interface()
		found := false
		for _, val := range got {
			if reflect.TypeOf(val) == reflect.TypeOf(want) && reflect.DeepEqual(val, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("OTLPFlags.%s (%v) is not reported by SummaryAttrs", v.Type().Field(i).Name, want)
		}
	}
}
