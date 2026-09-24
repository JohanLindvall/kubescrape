package servicegraph

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// A shard client narrates its destination's health like every OTLP client, and
// the line must name what it is talking to: a sibling SHARD, by name. Called
// "the OTLP collector" (otlpexport's default), a failing internal hop sent the
// operator to the collector's workload while the shard was the one down.
func TestShardClientHealthLinesNameTheShard(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	down := ln.Addr().String()
	_ = ln.Close() // nothing listens there now: every dial is refused

	log, dump := capturedLog()
	base := otlpexportConfigForTest()
	base.Timeout = 5 * time.Second
	r, err := NewResharder(ReshardConfig{Self: "self:4319", Endpoints: []string{down, "self:4319"}}, base, log)
	if err != nil {
		t.Fatalf("NewResharder: %v", err)
	}
	defer func() { _ = r.Close() }()
	c, ok := r.clients[down]
	if !ok {
		t.Fatalf("no client for the remote shard %q", down)
	}
	if err := c.ExportTraces(context.Background(), realisticBatch(1, 0)); err == nil {
		t.Fatal("an export to a closed port succeeded; the test exercises nothing")
	}
	out := dump()
	if !strings.Contains(out, "a trace-tier shard") || !strings.Contains(out, "shard="+down) {
		t.Errorf("the shard client's health line does not name the shard:\n%s", out)
	}
	if strings.Contains(out, "the OTLP collector") {
		t.Errorf("the shard client's health line calls a sibling shard the OTLP collector:\n%s", out)
	}
}
