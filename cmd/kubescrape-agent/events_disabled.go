//go:build !events

package main

import "context"

// eventsBuilt reports that this build does NOT contain the Kubernetes events
// reader: the `events` build tag was not set.
//
// This file is the whole tag-less build. Because it does not import
// internal/agent/events or internal/leader, the ENTIRE Kubernetes client stays
// out of the binary — 412 k8s.io/sigs.k8s.io packages, half the shipped
// agent's code, for a pipeline that only ever runs in the one-replica
// singleton Deployment. Measured on the shipped shape
// (-trimpath -ldflags="-s -w", TAGS=journald,azure): 59,141,096 bytes with the
// tag, 29,095,624 without it — 28.65 MiB, 50.8%.
//
// The Makefile's TAGS default puts the tag back, so `make build` and
// `make image` are unchanged; a bare `go build` lands here.
const eventsBuilt = false

// validateEventsFlags has nothing to check here: -events-start is the tuning
// surface of a pipeline this binary refuses to start at all
// (checkExcludedPipelines, from validateConfig), and ValidateStartMode lives in
// the package that is not linked. Same one behaviour difference the azure stub
// carries: an invalid -events-start passed WITHOUT -events is not diagnosed.
func validateEventsFlags() error { return nil }

// startEvents refuses to start a pipeline this binary does not contain.
// Unreachable in practice (validateConfig raises the same error first, from
// both a real start and -check-config); see startJournald's stub.
func (p *pipelines) startEvents(context.Context) error {
	if !*eventsOn {
		return nil
	}
	return excludedPipelineErrorFor("events")
}
