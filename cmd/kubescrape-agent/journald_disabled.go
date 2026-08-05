//go:build !journald

package main

import "context"

// journaldBuilt reports that this build does NOT contain the journal reader:
// the `journald` build tag was not set.
//
// This file is the whole tag-less build: because it does not import
// internal/agent/journald, the linker never sees
// coreos/go-systemd/v22/sdjournal, and with it goes the ONE cgo dependency in
// the agent — so this binary builds under CGO_ENABLED=0, links statically, and
// needs neither libsystemd nor its six transitive .so files in the image.
//
// The Makefile's TAGS default puts the tag back, so `make build` and
// `make image` are unchanged; a bare `go build` lands here.
const journaldBuilt = false

// startJournald refuses to start a pipeline this binary does not contain.
//
// Unreachable in practice — validateConfig raises the same error first, from
// both a real start and -check-config — but it is what keeps the refusal true
// no matter how the pipeline is reached, and it is cheaper than a fake reader
// that would silently export nothing.
func (p *pipelines) startJournald(context.Context) error {
	if !*journaldOn {
		return nil
	}
	return excludedPipelineErrorFor("journald")
}
