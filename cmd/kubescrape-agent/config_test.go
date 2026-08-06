package main

// -check-config is the promise that a config which passes will start. These
// pin the places where the dry run drifted from what a real start does.

import (
	"bytes"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

// -check-config must validate what BuildExporter actually BUILDS, not just the
// section's shape and the flag base. Every rule that only becomes checkable
// after the per-signal merge was invisible to the dry run, so the check whose
// purpose is preventing a fleet-wide CrashLoop passed configs that produced
// one at `creating OTLP exporter`.
func TestCheckConfigValidatesMergedExportDestinations(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  agentConfig
		want string
	}{
		{
			// TLS material on a destination that inherits plaintext gRPC from
			// the flags: only visible after the merge.
			name: "ca file on an inherited plaintext destination",
			cfg: agentConfig{Export: &otlpexport.ExportConfig{
				Logs: &otlpexport.ExportOverride{Endpoint: "loki:4317", CAFile: "/etc/ca.pem"},
			}},
			want: "export.logs",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfig(tc.cfg, "")
			if err == nil {
				t.Fatal("-check-config accepted a config a real start refuses")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name the offending destination (%q)", err, tc.want)
			}
		})
	}
}

// The -check-config summary answers "is this what I meant?", so a section it
// does not print reads as "not configured". Its list was thirteen hand-spelled
// lines beside the reflect-derived one the -config help uses — the drift that
// walk was added to end. Both come from the struct now, and this asserts the
// PRINTED list is the declared one.
func TestSummaryPrintsEveryDeclaredSection(t *testing.T) {
	cfg := agentConfig{}
	v := reflect.ValueOf(&cfg).Elem()
	for i := range v.NumField() {
		f := v.Field(i)
		if f.Kind() != reflect.Pointer {
			t.Fatalf("field %q is not a pointer; a section's presence is its nil-ness",
				v.Type().Field(i).Name)
		}
		f.Set(reflect.New(f.Type().Elem())) // every section present
	}

	var buf bytes.Buffer
	printConfigSummary(cfg, slog.New(slog.NewTextHandler(&buf, nil)))
	for _, name := range strings.Split(configSections(), ", ") {
		if !strings.Contains(buf.String(), name) {
			t.Errorf("section %q is declared but missing from the -check-config summary:\n%s", name, buf.String())
		}
	}
	if len(presentSections(agentConfig{})) != 0 {
		t.Fatal("an empty config reported sections")
	}
}
