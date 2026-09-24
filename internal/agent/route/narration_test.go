package route

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

// A route destination is narrated ONCE. The production destination is an
// *otlpexport.Client, which reports every wire send it makes; the router used
// to narrate the same result again, so a tenant outage produced two transition
// warnings (and two re-warns, two recoveries) — one of them calling a
// tenant's collector "the OTLP collector". The client now carries the route's
// name (otlpexport.WithReport) and the router stays quiet for it.
//
// Deliberately NOT parallel: both narrators log through slog.Default() when
// given no logger, so capturing the second one means swapping the process
// default, which only a sequential test may do.
func TestARouteDestinationIsNarratedOnce(t *testing.T) {
	log, dump := capturedLogger()
	prev := slog.Default()
	slog.SetDefault(log)
	defer slog.SetDefault(prev)

	// A port nothing listens on.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := srv.URL
	srv.Close()

	rc, err := otlpexport.New(otlpexport.Config{Endpoint: endpoint, Protocol: "http", Compression: "none", Timeout: time.Second, RetryAttempts: 1},
		otlpexport.WithReport(nil, "a routing destination", "route", "tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	r := New(&capDest{}, []Destination{{Name: "tenant-a", Namespaces: []string{"app-*"}, Exporter: rc}})

	if err := r.ExportLogs(context.Background(), nsLogs("app-one")); err == nil {
		t.Fatal("a route to a closed port must fail the export")
	}
	out := dump()
	if n := strings.Count(out, "are failing"); n != 1 {
		t.Fatalf("want ONE failure line for one failing route destination, got %d:\n%s", n, out)
	}
	for _, want := range []string{"exports to a routing destination are failing", "route=tenant-a", "endpoint=" + endpoint} {
		if !strings.Contains(out, want) {
			t.Errorf("the one line must say which route AND which endpoint; missing %q:\n%s", want, out)
		}
	}

	// A destination that does not narrate itself — anything but a Client —
	// keeps the router's narrator, or its outages would be silent.
	fake := New(&capDest{}, []Destination{{Name: "fake", Namespaces: []string{"x"}, Exporter: &capDest{}}})
	if fake.health[0] == nil {
		t.Error("a destination that does not report its own health lost the router's narrator")
	}
	if r.health[0] != nil {
		t.Error("the router kept a second narrator for a destination that narrates itself")
	}
}
