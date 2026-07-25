package main

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
)

// -check-config is only useful if it rejects everything a real start rejects.
// Both go through this one function, so they cannot drift.
func TestValidateConfigRejectsBadSections(t *testing.T) {
	cases := []struct {
		name string
		cfg  agentConfig
		want string
	}{
		{
			"unknown scrub pattern",
			agentConfig{LogScrubbing: &logscrub.Config{Builtin: []string{"no-such"}}},
			"logScrubbing",
		},
		{
			"malformed routing glob",
			agentConfig{Routing: &route.Config{Routes: []route.Route{
				{Name: "team-a", Namespaces: []string{"team-[a"}},
			}}},
			"routing",
		},
		{
			"route without namespaces",
			agentConfig{Routing: &route.Config{Routes: []route.Route{{Name: "x"}}}},
			"namespaces are required",
		},
		{
			"malformed trace-sampling duration",
			agentConfig{TraceSampling: &tracesample.Config{Probability: 0.5, KeepSlowerThan: "2quarters"}},
			"traceSampling",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfig(tc.cfg, "")
			if err == nil {
				t.Fatalf("validateConfig accepted an invalid config")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name the offending section (%q)", err, tc.want)
			}
		})
	}
}

func TestValidateConfigAcceptsEmpty(t *testing.T) {
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("an empty config must be valid: %v", err)
	}
}
