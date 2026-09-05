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
// singleton Deployment. Measured on the shipped shape (-trimpath
// -ldflags="-s -w", CGO_ENABLED=1, go1.26.6, re-measured 2026-08-29):
// 59,157,640 bytes with the tag, 26,268,328 without it — 31.37 MiB, 55.6%.
// buildtags.go carries the measurement and its history; this figure must
// track that one.
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
