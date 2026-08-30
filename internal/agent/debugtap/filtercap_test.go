package debugtap

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// The attack this keeps refuted, from the third adversarial review.
//
// Any pod in the cluster reaches the agent's -listen port (the shipped
// NetworkPolicy declares the port and carries no `from` selector). The filters
// of one /debug/otlp stream are ANDed and matchAll short-circuits only on a
// filter that does NOT match, so a long list of filters chosen to ALL match
// walks the resource's whole attribute map once per filter — on the exporting
// goroutine, which for logs is the tailer's single sweep goroutine serving
// every log file on the node. maxSubscribers=4 bounds the number of streams
// and nothing about the work each one asks for: measured at 3.2 s of CPU per
// export per stream at the stdlib's 10,000-parameter ceiling, against the
// ~23 ms the subscriber cap was sized for.
func TestAttrFilterCountIsBounded(t *testing.T) {
	tap := New(&fakeInner{})
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()

	// The attacker's query: thousands of filters that all match, so every one
	// of them is evaluated against every attribute of every resource.
	q := make(url.Values)
	for i := 0; i < 5000; i++ {
		q.Add("attr", "*/*=*")
	}
	resp, err := http.Get(srv.URL + "?signal=logs&" + q.Encode())
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 512)
	n, _ := resp.Body.Read(body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("5000-filter stream = %d, want 400", resp.StatusCode)
	}
	// The refusal must name the limit: a refusal that reads as a bug gets
	// configured away, and this one is the operator's own debugging endpoint.
	if msg := string(body[:n]); !strings.Contains(msg, fmt.Sprint(maxAttrFilters)) {
		t.Errorf("refusal does not name the limit: %q", msg)
	}
	// And it must cost NOTHING per export: the subscribe happens after this
	// check, so no render is ever paid for the refused request.
	if got := tap.active.Load(); got != 0 {
		t.Fatalf("refused stream still attached: active=%d", got)
	}
}

// One past the cap is refused, exactly the cap is served: the bound must be
// where the comment says it is, and a real debugging session must still work.
func TestAttrFilterCapBoundaries(t *testing.T) {
	tap := New(&fakeInner{})
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()

	get := func(filters int) *http.Response {
		t.Helper()
		q := make(url.Values)
		q.Set("signal", "logs")
		for i := 0; i < filters; i++ {
			q.Add("attr", fmt.Sprintf("k8s.namespace.name=team-%d*", i))
		}
		req, err := http.NewRequest("GET", srv.URL+"?"+q.Encode(), nil)
		if err != nil {
			t.Fatal(err)
		}
		// The stream never ends on its own; cancelling the request context is
		// how a reader detaches.
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		resp, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := get(maxAttrFilters + 1)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("%d filters = %d, want 400", maxAttrFilters+1, resp.StatusCode)
	}

	resp = get(maxAttrFilters)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%d filters = %d, want 200", maxAttrFilters, resp.StatusCode)
	}
}

// The SIZE half of the same ceiling. maxAttrFilters bounds how many globs an
// export is walked against; without a bound on their LENGTH the only ceiling
// on one glob was net/http's 1 MiB request line, and the cost of a glob is
// linear in its length (path.Match is O(pattern x name), and globMatch copies
// a pattern containing '/' once per comparison) — paid per resource attribute,
// per resource, per export, on the exporting goroutine, which for logs is the
// tailer's single sweep goroutine serving every log file on the node. So the
// count bound could be respected exactly and the multiplier kept: a handful of
// 100 KiB globs instead of 5,000 six-byte ones. The only ceiling those had was
// net/http's own 1 MiB request line — a bound on the REQUEST, not on the work
// it asks for, and 200x this endpoint's own per-filter bound.
func TestAttrFilterSizeIsBounded(t *testing.T) {
	tap := New(&fakeInner{})
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()

	q := make(url.Values)
	q.Set("signal", "logs")
	// Well inside maxAttrFilters, every glob matches everything, and the whole
	// query still fits the stdlib's 1 MiB request line — so what refuses this
	// has to be the endpoint's own ceiling, not net/http's.
	for range 8 {
		q.Add("attr", "*"+strings.Repeat("a", 100<<10)+"*=*")
	}
	req, err := http.NewRequest("GET", srv.URL+"?"+q.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// The refusal must be a status, so the stream can never attach; a request
	// that DID attach would hang this test, which is the point of asserting
	// active==0 below as well.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r2, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 512)
	n, _ := r2.Body.Read(body)
	_ = r2.Body.Close()
	if r2.StatusCode != http.StatusBadRequest {
		t.Fatalf("eight 100 KiB globs = %d, want 400", r2.StatusCode)
	}
	if msg := string(body[:n]); !strings.Contains(msg, fmt.Sprint(maxAttrFilterBytes)) {
		t.Errorf("refusal does not name the limit: %q", msg)
	}
	if got := tap.active.Load(); got != 0 {
		t.Fatalf("refused stream still attached: active=%d", got)
	}
}

// One byte past the ceiling is refused, exactly the ceiling is served: a real
// debugging session must still be able to write a long glob.
func TestAttrFilterSizeBoundaries(t *testing.T) {
	tap := New(&fakeInner{})
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()

	get := func(globBytes int) *http.Response {
		t.Helper()
		const key = "k8s.namespace.name="
		q := make(url.Values)
		q.Set("signal", "logs")
		q.Add("attr", key+strings.Repeat("a", globBytes-len(key)))
		req, err := http.NewRequest("GET", srv.URL+"?"+q.Encode(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		resp, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := get(maxAttrFilterBytes + 1)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("a %d-byte filter = %d, want 400", maxAttrFilterBytes+1, resp.StatusCode)
	}

	resp = get(maxAttrFilterBytes)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a %d-byte filter = %d, want 200", maxAttrFilterBytes, resp.StatusCode)
	}
}
