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

// A container lookup has TWO parking spots and -max-blocked-lookups promises to
// bound "how many container lookups may be blocked waiting for metadata at
// once". It used to bound one of them. The uncovered one was waitReady — the
// readiness park, which holds a request for its whole wait budget while the
// informer caches are still syncing — and that is the STARTUP window, when a
// whole agent fleet is likeliest to be asking at once: measured, 1200
// concurrent lookups all parked there at a cap of 4, 71 MB of parked handlers,
// with the blocked-lookup gauge reading 0 the whole time.
//
// So the budget is one budget: the readiness park draws a slot from the store's
// cap (Store.TryPark) and is refused the same way — 503 + Retry-After, counted
// on kubescrape_container_lookups_shed_total.
func TestLookupsParkedOnTheInitialSyncSpendTheBlockedLookupBudget(t *testing.T) {
	const capacity = 4
	const over = 6

	st := store.New(time.Minute)
	st.SetMaxWaiters(capacity)
	api := New(Config{
		Store:    st,
		Services: services.NewIndex(),
		Resolver: stubResolver{},
		MaxWait:  30 * time.Second,
		Ready:    make(chan struct{}), // never closed: the caches never sync
	})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	type result struct {
		code       int
		retryAfter string
		body       string
		err        error
	}
	// A client timeout well under the 30s wait budget: a request that PARKS
	// where it should have been refused has to come back as a failure with a
	// message, not as a hung test.
	client := &http.Client{Timeout: 10 * time.Second}
	get := func(id string) result {
		resp, err := client.Get(srv.URL + "/v1/containers/" + id + "?wait=30s")
		if err != nil {
			return result{err: err}
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return result{code: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After"), body: string(b)}
	}

	// Fill the budget. These are legitimate: they park, and Drain answers them
	// at the end.
	var wg sync.WaitGroup
	for i := range capacity {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = get(strings.Repeat(string(rune('a'+i)), 64))
		}()
	}
	deadline := time.Now().Add(10 * time.Second)
	for api.readyParked.Load() < capacity {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d lookups parked on the readiness gate", api.readyParked.Load(), capacity)
		}
		time.Sleep(time.Millisecond)
	}
	// The gauge has to see them, or an operator watching pressure build reads 0
	// through the entire window in which these are the only parked lookups.
	if got := st.BlockedLookups(); got != capacity {
		t.Errorf("BlockedLookups = %d while %d lookups sit in the readiness park: "+
			"kubescrape_container_lookups_blocked is blind to the spot that is full", got, capacity)
	}

	// Everything past the budget is refused, retryably, and counted.
	shedBefore := st.ShedLookups()
	for i := range over {
		got := get(strings.Repeat(string(rune('A'+i)), 64))
		switch {
		case got.err != nil:
			t.Fatalf("lookup %d over the cap of %d never came back (%v): the readiness park is uncapped, "+
				"so -max-blocked-lookups bounds the store's waiters and nothing else — and the readiness park "+
				"is the startup window, when an agent fleet is most likely to be hammering this route at once",
				i, capacity, got.err)
		case got.code != http.StatusServiceUnavailable:
			t.Fatalf("lookup %d over the cap: status %d, want 503; body %q", i, got.code, got.body)
		case got.retryAfter == "":
			t.Errorf("lookup %d over the cap: 503 without Retry-After — the agent cannot tell a shed "+
				"apart from a hard failure", i)
		case !strings.Contains(got.body, store.ErrTooManyWaiters.Error()):
			t.Errorf("lookup %d over the cap: body %q, want it to name the cap rather than the sync: "+
				"the two refusals mean different things to whoever is holding the request", i, got.body)
		}
	}
	if got := st.ShedLookups() - shedBefore; got != over {
		t.Errorf("ShedLookups moved by %d, want %d: the refusal is the operator's only signal that the "+
			"cap bound, and it must not depend on WHICH of the two spots the request had reached", got, over)
	}
	// The refusals must not have consumed budget of their own.
	if got := api.readyParked.Load(); got != capacity {
		t.Errorf("readyParked = %d after %d refusals, want %d: a refused lookup parked anyway", got, over, capacity)
	}

	api.Drain()
	wg.Wait()
}

// …and the slot is RETURNED. A readiness park that leaked its slot would shed
// every later lookup for the life of the process — the caches sync, the parks
// drain, and the cap stays spent.
func TestReadinessParksReturnTheirSlot(t *testing.T) {
	st := store.New(time.Minute)
	st.SetMaxWaiters(1)
	// Resolvable, so once the caches "sync" the lookup is ANSWERED rather than
	// parking again in the store: what is under test is the slot the readiness
	// park took, and a second park would hold one legitimately.
	addPod(st)
	ready := make(chan struct{})
	api := New(Config{
		Store:    st,
		Services: services.NewIndex(),
		Resolver: stubResolver{},
		MaxWait:  30 * time.Second,
		Ready:    ready,
	})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	client := &http.Client{Timeout: 10 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := client.Get(srv.URL + "/v1/containers/cafe01?wait=30s")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for api.readyParked.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the lookup never parked on the readiness gate")
		}
		time.Sleep(time.Millisecond)
	}
	close(ready) // the caches sync; the park is released and the lookup goes on
	<-done

	if got := st.BlockedLookups(); got != 0 {
		t.Fatalf("BlockedLookups = %d once the readiness park has drained, want 0: the slot was never "+
			"returned, so the cap stays spent for the life of the process", got)
	}
	if !st.TryPark() {
		t.Fatal("the budget is still spent after the readiness park drained")
	}
	st.Unpark()
}
