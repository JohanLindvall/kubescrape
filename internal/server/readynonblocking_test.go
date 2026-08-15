package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// store.GetContainer refuses to pay for the waiter protocol on behalf of a
// lookup that cannot block ("A lookup that cannot block must not pay for the
// waiter protocol", lookup.go): an already-done context skips the registration
// rather than taking the exclusive lock twice for a channel nothing can wait
// on. waitReady is the SPOT THAT IS REACHED FIRST and it did not carry the same
// guard, so during the initial sync a `?wait=0` lookup parked, was counted
// against the blocked-lookup cap, and could be shed by it.
//
// That is the agent's ordinary shape, not a hostile one: the cadvisor batcher
// asks metaclient.Container(ctx, id, 0) — `?wait=0s` on the wire — and so do
// two ingest-enricher paths, with -cadvisor on by default. So a restart of the
// metadata service put every node's non-blocking lookups into the readiness
// park alongside the blocking ones, where they can spend the slots the cap
// exists to reserve for requests that actually wait, and each refusal moves
// kubescrape_container_lookups_shed_total — the counter CLAUDE.md deliberately
// keeps apart from the drained one so that "a rolling update must not page like
// an abuse event".
func TestNonBlockingLookupsDoNotSpendTheReadinessParkBudget(t *testing.T) {
	st := store.New(time.Minute)
	// One slot, held below by a genuine blocking lookup: the cap is what makes
	// a stolen slot observable. Everything asserted here is about the wait=0
	// request, which must never be looking at this budget at all.
	st.SetMaxWaiters(1)
	api := New(Config{
		Store:    st,
		Services: services.NewIndex(),
		Resolver: stubResolver{},
		MaxWait:  30 * time.Second,
		Ready:    make(chan struct{}), // never closed: the caches never sync
	})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	client := &http.Client{Timeout: 10 * time.Second}
	get := func(query string) (int, string) {
		resp, err := client.Get(srv.URL + "/v1/containers/" + strings.Repeat("a", 64) + query)
		if err != nil {
			t.Fatalf("GET %s: %v", query, err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	// The genuine blocking lookup: it parks on the readiness gate and holds the
	// only slot until Drain answers it.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := client.Get(srv.URL + "/v1/containers/" + strings.Repeat("b", 64) + "?wait=30s")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for api.readyParked.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the blocking lookup never parked on the readiness gate")
		}
		time.Sleep(time.Millisecond)
	}

	shedBefore := st.ShedLookups()
	code, body := get("?wait=0")

	// It is still a 503 — the caches are not synced, so a 404 would be a lie
	// about a cache that has not been read yet — but it must be the SYNC's 503,
	// not the cap's.
	if code != http.StatusServiceUnavailable {
		t.Fatalf("wait=0 during the initial sync: status %d, want 503; body %q", code, body)
	}
	if strings.Contains(body, store.ErrTooManyWaiters.Error()) {
		t.Errorf("wait=0 during the initial sync was refused by the blocked-lookup cap (%q): a lookup that "+
			"declined to block took a slot from the ones that did, and the agent is told the service is "+
			"overloaded when it is merely still syncing", body)
	}
	if !strings.Contains(body, errNotSynced.Error()) {
		t.Errorf("wait=0 during the initial sync: body %q, want it to name the sync — that is the condition "+
			"this pod is actually in", body)
	}
	if got := st.ShedLookups() - shedBefore; got != 0 {
		t.Errorf("ShedLookups moved by %d for a lookup that cannot block: kubescrape_container_lookups_shed_total "+
			"is the abuse signal, and every agent's ?wait=0 cadvisor lookup would move it through the whole of "+
			"a rolling update", got)
	}
	// The blocking park is untouched: what was refused above must not have been
	// the OTHER request's slot being released and retaken.
	if got := api.readyParked.Load(); got != 1 {
		t.Errorf("readyParked = %d after the non-blocking lookup, want 1", got)
	}

	api.Drain()
	wg.Wait()
}

// …and the guard must not cost the wait=0 lookup its slot-free path once the
// caches ARE synced: with the cap full it is answered normally, because
// waitReady returns before any of this.
func TestNonBlockingLookupIsUnaffectedOnceSynced(t *testing.T) {
	st := store.New(time.Minute)
	st.SetMaxWaiters(1)
	ready := make(chan struct{})
	close(ready)
	api := New(Config{
		Store:    st,
		Services: services.NewIndex(),
		Resolver: stubResolver{},
		MaxWait:  30 * time.Second,
		Ready:    ready,
	})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	shedBefore := st.ShedLookups()
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(
		srv.URL + "/v1/containers/" + strings.Repeat("c", 64) + "?wait=0")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("wait=0 against a synced empty store: status %d, want 404; body %q", resp.StatusCode, b)
	}
	if got := st.ShedLookups() - shedBefore; got != 0 {
		t.Errorf("ShedLookups moved by %d for a synced non-blocking lookup", got)
	}
}
