package main

// -check-config is the promise that a config which passes will start. These
// pin the places where the dry run drifted from what a real start does.

import (
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
