package main

import (
	"bytes"
	"flag"
	"os"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/docscheck"
)

var updateConfigSchema = flag.Bool("update-config-schema", false, "rewrite docs/agent-config.schema.json from the config structs")

const configSchemaPath = "../../docs/agent-config.schema.json"

// docs/agent-config.schema.json is generated from the agentConfig struct tree
// — the very types -config decodes into — so editors validating against it
// (yaml-language-server) and CI reject exactly what UnmarshalStrict would.
// Any config-surface change fails here until the schema is regenerated:
//
//	go test ./cmd/kubescrape-agent -run TestConfigSchemaIsCurrent -update-config-schema
func TestConfigSchemaIsCurrent(t *testing.T) {
	want, err := docscheck.ConfigSchema(agentConfig{},
		"kubescrape-agent -config",
		"Unified agent configuration (the -config YAML). Generated from the Go config structs; "+
			"regenerate with: go test ./cmd/kubescrape-agent -run TestConfigSchemaIsCurrent -update-config-schema. "+
			"Structural only — semantic validation (regex compilation, duration strings, bounds) is -check-config's. "+
			"See docs/CONFIGURATION.md for what the fields mean.")
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, '\n')
	got, err := os.ReadFile(configSchemaPath)
	if *updateConfigSchema {
		if err := os.WriteFile(configSchemaPath, want, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err != nil {
		t.Fatalf("%v — regenerate with: go test ./cmd/kubescrape-agent -run TestConfigSchemaIsCurrent -update-config-schema", err)
	}
	if !bytes.Equal(got, want) {
		t.Error("docs/agent-config.schema.json is stale; regenerate with: go test ./cmd/kubescrape-agent -run TestConfigSchemaIsCurrent -update-config-schema")
	}
}
