//go:build !azure

package main

import "context"

// azureBuilt reports that this build does NOT contain the Azure diagnostics
// consumer: the `azure` build tag was not set.
//
// This file is the whole tag-less build: because it does not import
// internal/agent/azurediag, eleven franz-go packages (plus their compression
// and SASL machinery) stay out of the binary — a Kafka client that only ever
// runs in the one-replica singleton Deployment, otherwise shipped in every
// DaemonSet image.
//
// The Makefile's TAGS default puts the tag back, so `make build` and
// `make image` are unchanged; a bare `go build` lands here.
const azureBuilt = false

// validateAzureFlags has nothing to check here: the -azure-* flags are the
// tuning surface of a pipeline this binary refuses to start at all
// (checkExcludedPipelines, from validateConfig), and their meaning lives in
// the package that is not linked. The one behaviour difference from a build
// with the tag is that an invalid -azure-start passed WITHOUT
// -azure-diagnostics is not diagnosed — it is a setting for a pipeline that
// cannot run.
func validateAzureFlags() error { return nil }

// startAzure refuses to start a pipeline this binary does not contain.
// Unreachable in practice (validateConfig raises the same error first, from
// both a real start and -check-config); see startJournald's stub.
func (p *pipelines) startAzure(context.Context) error {
	if !*azureOn {
		return nil
	}
	return excludedPipelineError("azure-diagnostics", "azure", "Azure diagnostics consumer")
}
