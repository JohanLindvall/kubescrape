package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/store"
)

// `/v1/containers/{id}?wait=` PARKS a handler for up to MaxWait, and the store
// caps how many may park at once so that an unauthenticated route cannot exhaust
// the process. That cap is a COUNT, and a count bounds memory only while each
// parked request costs about the same — which net/http's 1 MiB MaxHeaderBytes
// default made false: measured, one parked lookup carrying the widest header
// block that default admits retains 4.09 MB.
//
// maxHeaderBytes is one of the two things that make it true again (the other is
// releaseParkedHead, measured by waitercost_test.go). The refusal is net/http's
// own 431, before any handler runs — which is also why it is invisible to
// kubescrape_http_requests_total: nothing in this process sees the request.
func TestOversizedRequestHeadersAreRefused(t *testing.T) {
	srv := newAPI(store.New(time.Minute), time.Second).HTTPServer(":0")
	base := listenAndServe(t, srv)

	// Comfortably over the bound and far under net/http's 1 MiB default, so
	// this fails the moment MaxHeaderBytes goes back to being unset.
	req, err := http.NewRequest(http.MethodGet, base+"/v1/containers/abc?wait=0", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Padding", strings.Repeat("p", 64<<10))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("oversized-header request: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status %d for a 64 KiB header, want 431: the memory one parked lookup costs is the caller's to choose, "+
			"so the store's waiter cap bounds a count and not the process", resp.StatusCode)
	}
}

// The bound has to stay well clear of what this API legitimately sends, or it
// turns an agent's ordinary poll into a 431 nothing retries out of. Nothing
// below the byte bound is refused any more — a width refusal could never bound
// the cost it was written for (see maxHeaderBytes), so wide-but-small requests
// are served, and the parked cost is bounded by releasing the head instead.
func TestOrdinaryRequestHeadersArePassed(t *testing.T) {
	srv := newAPI(store.New(time.Minute), time.Second).HTTPServer(":0")
	base := listenAndServe(t, srv)

	for _, tc := range []struct {
		name string
		set  func(http.Header)
	}{
		{
			// The fattest header this API legitimately carries: a ServiceAccount
			// JWT on the authenticated route (~1 KB), plus an ETag and the usual
			// furniture.
			name: "an agent's poll",
			set: func(h http.Header) {
				h.Set("Authorization", "Bearer "+strings.Repeat("t", 2<<10))
				h.Set("If-None-Match", `"0123456789abcdef"`)
				h.Set("User-Agent", "kubescrape-agent/test")
			},
		},
		{
			// A service mesh plus a tracing sidecar plus a browser's own
			// furniture: several times what this API sees, and none of it a
			// reason to refuse.
			name: "a mesh's worth of forwarding headers",
			set: func(h http.Header) {
				for i := range 128 {
					h.Set(fmt.Sprintf("X-Forwarded-P%d", i), "some-value")
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, base+"/v1/containers/abc?wait=0", nil)
			if err != nil {
				t.Fatal(err)
			}
			tc.set(req.Header)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("ordinary request: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusRequestHeaderFieldsTooLarge {
				t.Fatalf("status 431 for an ordinary agent request: the header bound (%d bytes) "+
					"is below what this API sends", maxHeaderBytes)
			}
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status %d, want 404 (the container is unknown to an empty store)", resp.StatusCode)
			}
		})
	}
}

// A 431 must not cost the caller its connection budget in a way that hides the
// answer: the response is a real one, so a client sees the status rather than a
// reset. (net/http writes it and closes; this pins that we get to read it.)
func TestOversizedHeaderRefusalIsAReadableResponse(t *testing.T) {
	srv := newAPI(store.New(time.Minute), time.Second).HTTPServer(":0")
	base := listenAndServe(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/pods/ns/name", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Padding", strings.Repeat("p", 32<<10))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("no readable response to an oversized-header request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status %d, want 431", resp.StatusCode)
	}
}
