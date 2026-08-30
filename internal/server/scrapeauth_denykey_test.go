package server

// /v1/scrape-auth is the ONE authenticated route, and its warning tables are
// what an operator reads when a credential stops resolving. The allowlist MISS
// is keyed by three path segments the CALLER chose — nothing bounds their
// length or their number — so it must not share the table whose keys come from
// the allowlist: the RBAC-failure and non-UTF-8 warnings are the two an
// operator has to act on, and a bounded table that SUPPRESSES on saturation
// (internal/logdedupe's rule) is silenced by whoever fills it first.

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
)

func allowlistedMonitors(t *testing.T) *servicemonitors.Index {
	t.Helper()
	monitors := servicemonitors.NewIndex()
	if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "monitoring", "name": "web"},
		"spec": map[string]any{
			"selector": map[string]any{},
			"endpoints": []any{map[string]any{
				"port":              "http-metrics",
				"bearerTokenSecret": map[string]any{"name": "tok", "key": "token"},
			}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	return monitors
}

func TestAllowlistMissesCannotSuppressTheRealAuthFailures(t *testing.T) {
	srv, h := authFixture(t,
		erroringSecrets{err: errors.New("secrets is forbidden: RBAC")},
		allowlistedMonitors(t))

	// A caller holding the token mints refusals: distinct refs, and long ones.
	pad := strings.Repeat("q", 4096)
	for i := range 2 * maxScrapeAuthWarnRefs {
		get(t, fmt.Sprintf("%s/v1/scrape-auth/tenant/n%d%s/token", srv.URL, i, pad),
			"Bearer "+testScrapeToken)
	}

	// The failure an operator must see: an allowlisted ref whose Secret cannot
	// be read. Its key comes from the allowlist, so nothing a caller sends may
	// have taken its slot.
	if status, _ := get(t, srv.URL+"/v1/scrape-auth/monitoring/tok/token",
		"Bearer "+testScrapeToken); status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 from the failing secret read", status)
	}
	if got := h.matching("resolving scrape-auth secret"); len(got) != 1 {
		t.Errorf("the RBAC failure logged %d lines, want 1: a caller's refused refs filled the table it shares", len(got))
	}
	// And the clipped key is what the miss table holds, so a 4 KiB segment
	// cannot be retained per key either.
	for _, l := range h.matching("no indexed monitor endpoint") {
		if strings.Contains(l, pad) {
			t.Errorf("a refused ref reached the line unclipped")
			break
		}
	}
}
