package main

// startNodeInfo carries the readiness gate a DaemonSet rolling update advances
// on, and had no test at all — the gate release moved into selfmeta.Poll's
// OnFirst hook, where dropping it would leave every agent NotReady with the
// suite green.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func nodeMetaServer(t *testing.T, labels map[string]string) (*metaclient.Client, func() int) {
	t.Helper()
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.URL.Path != "/v1/nodes/node1/metadata" {
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(kubemeta.NodeMetadata{
			Name: "node1", ObjectMeta: kubemeta.ObjectMeta{Labels: labels},
		})
	}))
	t.Cleanup(srv.Close)
	return metaclient.New(metaclient.Config{Base: srv.URL, Timeout: 5 * time.Second}), func() int { return hits }
}

// The provider never yields nil — the node's NAME is known without the lookup,
// and attribute templates dereference .Node — and the readiness gate clears
// once the first fetch lands.
func TestStartNodeInfoResolvesAndClearsTheGate(t *testing.T) {
	meta, _ := nodeMetaServer(t, map[string]string{"topology.kubernetes.io/zone": "eu-1a"})
	ready := newReadiness()
	ready.require(gateMetadata)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	info := startNodeInfo(ctx, meta, "node1", 50*time.Millisecond, discardLogger(), ready)
	if n := info(); n == nil || n.Name != "node1" {
		t.Fatalf("provider = %+v before the first fetch; want the bare node name", n)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if n := info(); n != nil && n.Labels["topology.kubernetes.io/zone"] == "eu-1a" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if n := info(); n == nil || n.Labels["topology.kubernetes.io/zone"] != "eu-1a" {
		t.Fatalf("provider = %+v; want the fetched labels", n)
	}
	if pending := ready.pending(); len(pending) != 0 {
		t.Fatalf("readiness still pending %v; the metadata gate must clear on the first successful fetch", pending)
	}
}

// -node-metadata-refresh=0 disables the lookup: nothing is fetched, the
// provider still yields the bare name, and the gate is never registered (run()
// only requires it when the refresh is positive).
func TestStartNodeInfoZeroRefreshMakesNoRequest(t *testing.T) {
	meta, hits := nodeMetaServer(t, nil)
	ready := newReadiness()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	info := startNodeInfo(ctx, meta, "node1", 0, discardLogger(), ready)
	time.Sleep(30 * time.Millisecond)
	if n := info(); n == nil || n.Name != "node1" || len(n.Labels) != 0 {
		t.Fatalf("provider = %+v; want the bare node name", n)
	}
	if hits() != 0 {
		t.Fatalf("%d requests with the lookup disabled", hits())
	}
}
