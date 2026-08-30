package server

// The unresolved-endpoint warning's dedupe key is a security boundary, for the
// reason internal/agent/promscrape's warnOnce spells out: the table is bounded
// and SATURATION SUPPRESSES, so whatever can mint distinct keys can stop the
// warning reporting anyone else's broken endpoint. A monitor's `port` spelling
// is CR content — one object may declare as many endpoints as fit in it, each
// naming a string of whatever length its author picked — so it belongs on the
// line, clipped, and not in the key.

import (
	"strconv"
	"strings"
	"testing"
)

func TestUnresolvedEndpointKeyIsIdentityNotCRContent(t *testing.T) {
	const endpoints = 50
	pad := strings.Repeat("z", 300)
	eps := make([]any, 0, endpoints)
	for i := range endpoints {
		// Every endpoint names a port no container declares, spelled
		// differently and at a length the CR's author chose.
		eps = append(eps, map[string]any{"port": "typo-" + strconv.Itoa(i) + "-" + pad})
	}
	s, h := capWarnFixture(t, eps)

	if targets, _ := s.nodeTargets("node1"); len(targets) != 0 {
		t.Fatalf("the fixture must resolve to nothing; got %d targets", len(targets))
	}

	lines := h.matching("names a port the selected pod does not declare")
	if len(lines) != 1 {
		t.Fatalf("want one warning for one monitor, got %d", len(lines))
	}
	if got := s.warnUnresolved.Len(); got != 1 {
		t.Errorf("one ServiceMonitor minted %d dedupe keys, want 1: CR content is in the key, so its author can saturate the table and suppress every other monitor's warning", got)
	}
	if strings.Contains(lines[0], pad) {
		t.Errorf("the CR's port spelling went onto the line unclipped (%d bytes of it)", len(pad))
	}
	if !strings.Contains(lines[0], "typo-0-") {
		t.Errorf("the clipped port lost the part an operator matches against the CR: %s", lines[0])
	}
}
