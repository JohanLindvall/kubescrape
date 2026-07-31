package metaclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Self hits /v1/self and is never served from the response cache: the answer
// depends on WHO asked, so a second call must reach the service again even if
// it (wrongly) sent cache headers.
func TestSelfIsNotCached(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/self" {
			t.Errorf("path = %q; want /v1/self", r.URL.Path)
		}
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"kubescrape-agent-xyz","namespace":"monitoring","uid":"agent-uid"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, 5*time.Second)
	ctx := context.Background()
	pod, err := c.Self(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Name != "kubescrape-agent-xyz" || pod.Namespace != "monitoring" {
		t.Fatalf("pod = %+v", pod)
	}
	if _, err := c.Self(ctx); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&hits); n != 2 {
		t.Fatalf("server hits = %d; want 2 (an uncached response per call)", n)
	}
}

// A caller the service cannot attribute to a live pod gets an error, not a
// zero-valued pod that would stamp empty attributes on everything.
func TestSelfUnresolved(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"no live pod with peer IP"}`, http.StatusNotFound)
	}))
	defer srv.Close()

	if pod, err := New(srv.URL, time.Second).Self(context.Background()); err == nil {
		t.Fatalf("Self returned %+v; want an error for an unattributable caller", pod)
	}
}
