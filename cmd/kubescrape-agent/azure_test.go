//go:build azure

package main

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/azurediag"
)

// The readiness gate keeps its historical BARE name while there is one
// consumer — deploy manifests, runbooks and the /readyz body all refer to it —
// and is qualified only once several consumers exist, where an unqualified
// name would let the first hub to poll satisfy the gate for a sibling nobody
// can read.
func TestAzureGateName(t *testing.T) {
	one := azurediag.KafkaConfig{
		Brokers: []string{"ns.servicebus.windows.net:9093"}, Topics: []string{"azure"},
	}
	if got := azureGateName([]azurediag.KafkaConfig{one}, one); got != gateAzure {
		t.Fatalf("single-source gate = %q, want the bare %q", got, gateAzure)
	}

	two := azurediag.KafkaConfig{
		Brokers: []string{"ns.servicebus.windows.net:9093"}, Topics: []string{"otherhub"},
	}
	all := []azurediag.KafkaConfig{one, two}
	gotOne, gotTwo := azureGateName(all, one), azureGateName(all, two)
	if gotOne == gotTwo {
		t.Fatalf("both consumers share gate %q: one hub polling would clear the other's", gotOne)
	}
	for _, g := range []string{gotOne, gotTwo} {
		if !strings.HasPrefix(g, gateAzure+"[") || !strings.HasSuffix(g, "]") {
			t.Fatalf("gate %q is not the qualified form", g)
		}
		if !strings.Contains(g, "ns.servicebus.windows.net") {
			t.Fatalf("gate %q does not name its namespace", g)
		}
	}
	if !strings.Contains(gotOne, "azure") || !strings.Contains(gotTwo, "otherhub") {
		t.Fatalf("gates do not name their hubs: %q / %q", gotOne, gotTwo)
	}
}

// The SHAPE half of the multi-source rules must fail -check-config, which runs
// validateAzureFlags before anything is acquired — a rollout should not be the
// thing that discovers an unresolvable namespace/connection-string mix.
func TestValidateAzureFlagsRejectsAmbiguousNamespaces(t *testing.T) {
	restore := func(on bool, ns, conn, start string) {
		*azureOn, *azureNamespace, *azureConnFile, *azureStart = on, ns, conn, start
	}
	defer restore(*azureOn, *azureNamespace, *azureConnFile, *azureStart)

	restore(true, "a.example,b.example", "/etc/cs1", "end")
	err := validateAzureFlags()
	if err == nil || !strings.Contains(err.Error(), "at most one namespace") {
		t.Fatalf("err = %v, want a refusal naming the ambiguity", err)
	}

	// One namespace alongside connection strings is the legal override.
	restore(true, "a.example", "/etc/cs1", "end")
	if err := validateAzureFlags(); err != nil {
		t.Fatalf("a single namespace override was refused: %v", err)
	}

	// Several namespaces with NO connection string is the managed-identity
	// multi-namespace shape, which is exactly what this change enables.
	restore(true, "a.example,b.example", "", "end")
	if err := validateAzureFlags(); err != nil {
		t.Fatalf("the managed-identity namespace list was refused: %v", err)
	}
}
